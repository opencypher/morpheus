package org.opencypher.spark.impl.logical

import org.neo4j.cypher.internal.frontend.v3_2.helpers.fixedPoint
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern.{AllGiven, Connection, Pattern}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.impl.{CompilationStage, DirectCompilationStage}

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
    producer.planSelect(fields.toSet, plan)
    // TODO: plan reorder for enforcing order and renaming
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
      case (acc, (f, p: Property)) =>
        val propType = propertyType(p, in)
        producer.projectField(f, p, acc)
      case (acc, (f, v: Var)) => acc
      case (_, x) => throw new UnsupportedOperationException(s"can not project $x")
    }
  }

  private def propertyType(p: Property, plan: LogicalOperator)(implicit context: LogicalPlannerContext): Option[CypherType] = {
    nodePropertyType(p, plan) orElse relPropertyType(p, plan)
  }

  private def nodePropertyType(p: Property, plan: LogicalOperator)(implicit context: LogicalPlannerContext): Option[CypherType] = {
    val labelsOnNode = plan.solved.predicates.collect {
      case h: HasLabel if h.node == p.m => h.label
    }

    // TODO: HeadOption? Move code into schema
    val propType = labelsOnNode.headOption.flatMap { ref =>
      val label = context.tokens.label(ref).name
      val keys = context.schema.nodeKeys(label)
      keys.get(context.tokens.propertyKey(p.key).name)
    }

    propType
  }

  private def relPropertyType(p: Property, plan: LogicalOperator)(implicit context: LogicalPlannerContext): Option[CypherType] = {
    val relTypes = plan.solved.predicates.collect {
      case h: HasType if h.rel == p.m => h.relType
    }
    val propType = relTypes.headOption.flatMap { ref =>
      val label = context.tokens.relType(ref).name
      val keys = context.schema.relationshipKeys(label)
      keys.get(context.tokens.propertyKey(p.key).name)
    }

    propType
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
        val propType = propertyType(p, in)
        val project = producer.projectExpr(p, in)
        project
      case Equals(expr1, expr2) =>
        val project1 = planInnerExpr(expr1, in)
        planInnerExpr(expr2, project1)
      case x => throw new NotImplementedError(s"Not yet able to plan projection for inner expression $x")
    }
  }

  private def planPattern(plan: LogicalOperator, pattern: Pattern[Expr])(implicit context: LogicalPlannerContext) = {
    val lhsLeaf = nodePlan(plan: LogicalOperator, pattern)

    val (newPlan, _) = fixedPoint(planExpansions)(lhsLeaf -> pattern)

    newPlan
  }

  private def planExpansions(input: (LogicalOperator, Pattern[Expr])): (LogicalOperator, Pattern[Expr]) = {
    val (in, remainingPattern) = input

    val solvedFields = in.solved.fields

    val result: Option[ExpandOperator] = remainingPattern.topology.collectFirst {
      case (r, c: Connection) =>
        solvedFields.collectFirst {
          case v if c.source == v =>
            producer.planSourceExpand(c.source, r -> remainingPattern.rels(r), c.target, in)
          case v if c.target == v =>
            producer.planTargetExpand(c.source, r, c.target, in)
        }
    }.flatten

    result match {
      case None => input
      case Some(op) => planExpansions(op -> remainingPattern.withoutConnection(Field(op.rel.name)()))
    }
  }

  private def nodePlan(plan: LogicalOperator, pattern: Pattern[Expr])(implicit context: LogicalPlannerContext) = {
    val (field, everyNode) = pattern.nodes.head

    producer.planNodeScan(field, everyNode, plan)
  }
}
