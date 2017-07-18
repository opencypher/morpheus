package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.pattern._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.DirectCompilationStage
import org.opencypher.spark.impl.exception.Raise

import scala.annotation.tailrec

final case class LogicalPlannerContext(defaultGraphSchema: Schema, inputRecordFields: Set[Var])

class LogicalPlanner(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[CypherQuery[Expr], LogicalOperator, LogicalPlannerContext] {

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
          block.after.find(r => !_plan.solved.contains(model(r))).getOrElse(Raise.logicalPlanningFailure())
      }
      val dependency = planBlock(depRef, model, plan)
      planBlock(ref, model, Some(dependency))
    }
  }

  def planLeaf(ref: BlockRef, model: QueryModel[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case LoadGraphBlock(_, DefaultGraph()) =>
        producer.planStart(context.defaultGraphSchema, context.inputRecordFields)
      case x =>
        Raise.notYetImplemented(s"leaf planning of $x")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where, graph) =>
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planPattern(plan, pattern)
        planFilter(patternPlan, where)
      case ProjectBlock(_, ProjectedFields(exprs), where, graph) =>
        val projPlan = planProjections(plan, exprs)
        planFilter(projPlan, where)
      case x =>
        Raise.notYetImplemented(s"logical planning of $x")
    }
  }

  private def planProjections(in: LogicalOperator, exprs: Map[Field, Expr])(implicit context: LogicalPlannerContext) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) => producer.projectField(f, p, acc)
      case (acc, (f, func: FunctionExpr)) => producer.projectField(f, func, acc)
        // this is for aliasing
      case (acc, (f, v: Var)) if f.name != v.name => producer.projectField(f, v, acc)
      case (acc, (_, _: Var)) => acc
      case (acc, (f, be: BinaryExpr)) =>
        val projectLhs = planInnerExpr(be.lhs, acc)
        val projectRhs = planInnerExpr(be.rhs, projectLhs)
        producer.projectField(f, be, projectRhs)
      case (acc, (f, c: Const)) => producer.projectField(f, c, acc)
      case (_, x) =>
        Raise.notYetImplemented(s"projection of $x")
    }
  }

  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(implicit context: LogicalPlannerContext) = {
    val filtersAndProjs = where.elts.foldLeft(in) {
      case (acc, eq: Equals) =>
        val project1 = planInnerExpr(eq.lhs, acc)
        val project2 = planInnerExpr(eq.rhs, project1)
        producer.planFilter(eq, project2)
      case (acc, be: BinaryExpr) =>
        val project1 = planInnerExpr(be.lhs, acc)
        val project2 = planInnerExpr(be.rhs, project1)
        // TODO: Come up with a strategy on whether to project filters or not -- or when/how?
        val projectParent = producer.projectExpr(be, project2)
        producer.planFilter(be, projectParent)
      case (acc, h@HasLabel(_: Var, _)) =>
        producer.planFilter(h, acc)
      case (acc, not@Not(Equals(lhs, rhs))) =>
        val p1 = planInnerExpr(lhs, acc)
        val p2 = planInnerExpr(rhs, p1)
        producer.planFilter(not, p2)
      case (acc, not@Not(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(not, project)
      case (acc, t: TrueLit) =>
        producer.planFilter(t, acc) // optimise away this one somehow... currently we do that in PhysicalPlanner
      case (acc, v: Var) =>
        producer.planFilter(v, acc)
      case (_, x) =>
        Raise.notYetImplemented(s"logical planning of predicate $x")
    }

    filtersAndProjs
  }

  private def planInnerExpr(expr: Expr, in: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    expr match {
      case _: Const => in
      case _: Var => in
      case p: Property =>
        producer.projectExpr(p, in)
      case be: BinaryExpr =>
        val project1 = planInnerExpr(be.lhs, in)
        val project2 = planInnerExpr(be.rhs, project1)
        producer.projectExpr(be, project2)
      case x =>
        Raise.notYetImplemented(s"projection of inner expression $x")
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
      planExpansions(nodePlans.toSet, pattern, producer)
    else if (nodePlans.size == 1) nodePlans.head
    else Raise.invalidPattern(pattern.toString)
  }

  @tailrec
  private def planExpansions(disconnectedPlans: Set[LogicalOperator], pattern: Pattern[Expr], producer: LogicalOperatorProducer): LogicalOperator = {
    val allSolved = disconnectedPlans.map(_.solved).reduce(_ ++ _)

    val (r, c) = pattern.topology.collectFirst {
      case (rel, conn: Connection) if !allSolved.solves(rel) => rel -> conn
    }.getOrElse(Raise.patternPlanningFailure())

    val sourcePlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.source) => p
    }.getOrElse(Raise.invalidConnection("source"))
    val targetPlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.target) => p
    }.getOrElse(Raise.invalidConnection("target"))

    val expand = c match {
      case v: VarLengthRelationship if v.upper.nonEmpty =>
        producer.planBoundedVarLengthExpand(c.source, r, pattern.rels(r), c.target, v.lower, v.upper.get, sourcePlan, targetPlan)
      case _ =>
        producer.planSourceExpand(c.source, r, pattern.rels(r), c.target, sourcePlan, targetPlan)
    }

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern, producer)
  }

  private def nodePlan(plan: LogicalOperator, field: Field, everyNode: EveryNode)(implicit context: LogicalPlannerContext) = {
    producer.planNodeScan(field, everyNode, plan)
  }
}
