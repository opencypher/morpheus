package org.opencypher.spark.prototype.impl.planner

import org.neo4j.cypher.internal.frontend.v3_2.helpers.fixedPoint
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.spark.prototype.impl.logical._

class LogicalOperatorProducer {

  def plan(ir: CypherQuery[Expr]): LogicalOperator = {
    val model = ir.model

    implicit val tokenDefs = model.globals

    planModel(model.result, model)
  }

  def planModel(block: ResultBlock[Expr], model: QueryModel[Expr])(implicit tokens: GlobalsRegistry): LogicalOperator = {
    val first = block.after.head // there should only be one, right?
    val plan = planBlock(first, model, None)

    // always plan a select at the top
    Select(block.binds.fieldsOrder.map(f => Var(f.name) -> f.name), plan)(plan.solved)
  }

  final def planBlock(ref: BlockRef, model: QueryModel[Expr], plan: Option[LogicalOperator])(implicit tokens: GlobalsRegistry): LogicalOperator = {
    val block = model(ref)
    if (block.after.isEmpty) {
      // this is a leaf block, just plan it
      planLeaf(ref, model)
    } else if (plan.nonEmpty && plan.get.solved.contains(block.after.map(model(_)))) {
      // all deps satisfied for this block, we can just plan it if we have already planned a leaf
      planNonLeaf(ref, model, plan.get)
    } else {
      // either we haven't planned a leaf yet, or the block is not ready to be planned
      // plan one of the block dependencies
      val depRef = plan match {
        case None =>
          // nothing has been planned, just pick one
          block.after.head
        case Some(_plan) =>
          // we need to plan a block that hasn't already been solved
          block.after.find(r => !_plan.solved.contains(model(r))).getOrElse(throw new IllegalStateException("Impossible because of above if case!"))
      }
      val dependency = planBlock(depRef, model, plan)
      planBlock(ref, model, Some(dependency))
    }
  }

  def planLeaf(ref: BlockRef, model: QueryModel[Expr])(implicit tokens: GlobalsRegistry): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where) =>
        // this plans a leaf + filter for convenience -- TODO
        val plan = planPattern(pattern)
        planFilter(plan, where)
      case x => throw new IllegalArgumentException(s"Don't know how to leaf-plan $x")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(implicit tokens: GlobalsRegistry): LogicalOperator = {
    model(ref) match {
      case ProjectBlock(_, ProjectedFields(exprs), _) =>
        planProjections(plan, exprs)
      case x => throw new IllegalArgumentException(s"Don't know how to plan $x")
    }
  }

  private def planProjections(in: LogicalOperator, exprs: Map[Field, Expr])(implicit tokens: GlobalsRegistry) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) =>
        Project(p, acc)(in.solved.withField(f))
      case (_, x) => throw new UnsupportedOperationException(s"can not project $x")
    }
  }

  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(implicit tokens: GlobalsRegistry) = {
    val equalities = where.elts.foldLeft(in) {
      case (acc, eq@Equals(prop: Property, _: Const)) =>
        val project = Project(prop, acc)(acc.solved)
        Filter(eq, project)(project.solved.withPredicate(eq))
      case (acc, h: HasLabel) =>
        Filter(h, acc)(acc.solved.withPredicate(h))
      case (_, x) => throw new UnsupportedOperationException(s"Can't deal with $x")
    }

    equalities
  }

  private def planPattern(pattern: Pattern[Expr])(implicit tokens: GlobalsRegistry) = {
    val lhsLeaf = nodePlan(pattern)

    val (newPlan, _) = fixedPoint(planExpansions)(lhsLeaf -> pattern)

      newPlan
  }

  @scala.annotation.tailrec
  private def planExpansions(input: (LogicalOperator, Pattern[Expr])): (LogicalOperator, Pattern[Expr]) = {
    val (in, remainingPattern) = input

    val knownVars = in.signature.items.flatMap(_.exprs.collect { case v: Var => v })

    val result: Option[ExpandOperator] = remainingPattern.topology.collectFirst {
      case (r, c) =>
        knownVars.collectFirst {
          case v if Var(c.source.name) == v => ExpandSource(Var(c.source.name), Var(r.name), Var(c.target.name), in)(in.solved.withFields(r, c.target))
          case v if Var(c.target.name) == v => ExpandTarget(Var(c.source.name), Var(r.name), Var(c.target.name), in)(in.solved.withFields(r, c.source))
        }
    }.flatten

    result match {
      case None => input
      case Some(op) => planExpansions(op -> remainingPattern.withoutConnection(Field(op.rel.name)))
    }
  }

  private def nodePlan(pattern: Pattern[Expr])(implicit tokens: GlobalsRegistry) = {
    val (field, everyNode) = pattern.nodes.head
    val node = Var(field.name)
    val solved = everyNode.labels.elts.foldLeft(SolvedQueryModel.empty[Expr].withField(field)) {
      case (acc, ref) => acc.withPredicate(HasLabel(node, ref))
    }
    NodeScan(node, everyNode)(solved)
  }
}
