package org.opencypher.spark.prototype.impl.planner

import org.neo4j.cypher.internal.frontend.v3_2.helpers.fixedPoint
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.logical._

final case class LogicalPlannerContext(schema: Schema, tokens: GlobalsRegistry)

class LogicalPlanner extends Stage[CypherQuery[Expr], LogicalOperator, LogicalPlannerContext] {

  val producer = new LogicalOperatorProducer

  def plan(ir: CypherQuery[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val model = ir.model

    implicit val tokenDefs = model.globals

    planModel(model.result, model)
  }

  def planModel(block: ResultBlock[Expr], model: QueryModel[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val first = block.after.head // there should only be one, right?
    val plan = planBlock(first, model, None)

    // always plan a select at the top
    val fields = block.binds.fieldsOrder.map(f => Var(f.name) -> f.name)
    producer.planSelect(fields, plan)
  }

  final def planBlock(ref: BlockRef, model: QueryModel[Expr], plan: Option[LogicalOperator])(implicit context: LogicalPlannerContext): LogicalOperator = {
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

  def planLeaf(ref: BlockRef, model: QueryModel[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where) =>
        // this plans a leaf + filter for convenience -- TODO
        val plan = planPattern(pattern)
        planFilter(plan, where)
      case x => throw new IllegalArgumentException(s"Don't know how to leaf-plan $x")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case ProjectBlock(_, ProjectedFields(exprs), _) =>
        planProjections(plan, exprs)
      case x => throw new IllegalArgumentException(s"Don't know how to plan $x")
    }
  }

  private def planProjections(in: LogicalOperator, exprs: Map[Field, Expr])(implicit context: LogicalPlannerContext) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) =>
        val propType = propertyType(p, in)
        producer.projectField(f, p, propType, acc)
      case (_, x) => throw new UnsupportedOperationException(s"can not project $x")
    }
  }

  private def propertyType(p: Property, plan: LogicalOperator)(implicit context: LogicalPlannerContext): Option[CypherType] = {
    val labelsOnNode = plan.solved.predicates.collect {
      case h: HasLabel if h.node == p.m => h.label
    }
    val propType = labelsOnNode.headOption.flatMap { ref =>
      val label = context.tokens.label(ref).name
      val keys = context.schema.nodeKeys(label)
      keys.get(context.tokens.propertyKey(p.key).name)
    }

    propType
  }

  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(implicit context: LogicalPlannerContext) = {
    val equalities = where.elts.foldLeft(in) {
      case (acc, eq@Equals(prop: Property, _: Const)) =>
        val propType = propertyType(prop, in)
        val project = producer.projectExpr(prop, propType, acc)
        producer.planFilter(eq, project)
      case (acc, h: HasLabel) =>
        producer.planFilter(h, acc)
      case (_, x) => throw new UnsupportedOperationException(s"Can't deal with $x")
    }

    equalities
  }

  private def planPattern(pattern: Pattern[Expr])(implicit context: LogicalPlannerContext) = {
    val lhsLeaf = nodePlan(pattern)

    val (newPlan, _) = fixedPoint(planExpansions)(lhsLeaf -> pattern)

    newPlan
  }

  private def planExpansions(input: (LogicalOperator, Pattern[Expr])): (LogicalOperator, Pattern[Expr]) = {
    val (in, remainingPattern) = input

    val solvedFields = in.solved.fields

    val result: Option[ExpandOperator] = remainingPattern.topology.collectFirst {
      case (r, c) =>
        solvedFields.collectFirst {
          case v if c.source == v =>
            producer.planSourceExpand(c.source, r, c.target, in)
          case v if c.target == v =>
            producer.planTargetExpand(c.source, r, c.target, in)
        }
    }.flatten

    result match {
      case None => input
      case Some(op) => planExpansions(op -> remainingPattern.withoutConnection(Field(op.rel.name)))
    }
  }

  private def nodePlan(pattern: Pattern[Expr])(implicit context: LogicalPlannerContext) = {
    val (field, everyNode) = pattern.nodes.head

    producer.planNodeScan(field, everyNode)
  }
}
