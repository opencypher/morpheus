package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.DirectCompilationStage

import scala.annotation.tailrec

final case class LogicalPlannerContext(schema: Schema, tokens: GlobalsRegistry)

class LogicalPlanner extends DirectCompilationStage[CypherQuery[Expr], LogicalOperator, LogicalPlannerContext] {

  val producer = new LogicalOperatorProducer

  override def process(ir: CypherQuery[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val model = ir.model

    implicit val tokenDefs = model.globals

    planModel(model.result, model)
  }

  def planModel(block: ResultBlock[Expr], model: QueryModel[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val first = block.after.head // there should only be one, right?
    val plan = planBlock(first, model, None)

    // always plan a select at the top
    val fields = block.binds.fieldsOrder.map(f => Var(f.name)(f.cypherType))
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
      case LoadGraphBlock(_, DefaultGraph()) =>
        producer.planLoadDefaultGraph(context.schema)
      case x =>
        throw new NotImplementedError(s"Not yet able to leaf-plan $x (only queries starting with MATCH or LOAD GRAPH are supported)")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where, graph) =>
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planPattern(plan, pattern)
        planFilter(patternPlan, where)
      case ProjectBlock(_, ProjectedFields(exprs), _, graph) =>
        planProjections(plan, exprs)
      case x => throw new IllegalArgumentException(s"Don't know how to plan $x")
    }
  }

  private def planProjections(in: LogicalOperator, exprs: Map[Field, Expr])(implicit context: LogicalPlannerContext) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) => producer.projectField(f, p, acc)
      case (acc, (_, _: Var)) => acc
      case (acc, (f, s: Subtract)) => producer.projectField(f, s, acc)
      case (_, x) => throw new UnsupportedOperationException(s"can not project $x")
    }
  }

  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(implicit context: LogicalPlannerContext) = {
    val filtersAndProjs = where.elts.foldLeft(in) {
      case (acc, eq@Equals(expr1, expr2)) =>
        val project1 = planInnerExpr(expr1, acc)
        val project2 = planInnerExpr(expr2, project1)
        producer.planFilter(eq, project2)
      case (acc, h@HasLabel(_: Var, l)) =>
        producer.planFilter(h, acc)
      case (acc, not@Not(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(not, project)
      case (acc, t: TrueLit) =>
        producer.planFilter(t, acc) // optimise away this one somehow... currently we do that in PhysicalPlanner
      case (_, x) => throw new NotImplementedError(s"Not yet able to plan filter using predicate $x")
    }

    filtersAndProjs
  }

  private def planInnerExpr(expr: Expr, in: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    expr match {
      case _: Const => in
      case _: Var => in
      case p: Property =>
        producer.projectExpr(p, in)
      case Equals(expr1, expr2) =>
        val project1 = planInnerExpr(expr1, in)
        planInnerExpr(expr2, project1)
      case x => throw new NotImplementedError(s"Not yet able to plan projection for inner expression $x")
    }
  }

  private def planPattern(plan: LogicalOperator, pattern: Pattern[Expr])(implicit context: LogicalPlannerContext) = {
    val nodes = pattern.entities.collect {
      case (f, e: EveryNode) if f.cypherType.subTypeOf(CTNode).isTrue => f -> e
    }

    val nodePlans = nodes.map {
      // TODO: This copies the full subtree underneath each node -- only a LoadGraph is necessary
      case (f, e) => nodePlan(plan, f, e)
    }

    if (pattern.topology.nonEmpty)
      planExpansions(nodePlans.toSet, pattern)
    else if (nodePlans.size == 1) nodePlans.head
    else throw new IllegalStateException("What kind of a pattern is this???")
  }

  @tailrec
  private def planExpansions(disconnectedPlans: Set[LogicalOperator], pattern: Pattern[Expr]): LogicalOperator = {
    val allSolved = disconnectedPlans.map(_.solved).reduce(_ ++ _)

    val (r, c) = pattern.topology.collectFirst {
      case (rel, conn: Connection) if !allSolved.solves(rel) => rel -> conn
    }.getOrElse(throw new IllegalStateException("Recursion / solved failure during logical planning: unable to find unsolved connection"))

    val sourcePlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.source) => p
    }.getOrElse(throw new IllegalStateException("A connection must have a known source!"))
    val targetPlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.target) => p
    }.getOrElse(throw new IllegalStateException("A connection must have a known target!"))

    val expand = producer.planSourceExpand(c.source, r -> pattern.rels(r), c.target, sourcePlan, targetPlan)

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern)
  }

  private def nodePlan(plan: LogicalOperator, field: Field, everyNode: EveryNode)(implicit context: LogicalPlannerContext) = {
    producer.planNodeScan(field, everyNode, plan)
  }
}
