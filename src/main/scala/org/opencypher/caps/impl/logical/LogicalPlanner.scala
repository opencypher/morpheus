/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.impl.logical

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.io.CAPSGraphSource
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.syntax.expr._
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block._
import org.opencypher.caps.ir.api.pattern._

import scala.annotation.tailrec

final case class LogicalPlannerContext(
  ambientGraphSchema: Schema,
  inputRecordFields: Set[Var],
  resolver: String => CAPSGraphSource
)

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
    val graphNames = block.binds.graphs.map(_.name)
    producer.planSelect(fields, graphNames, plan)
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
      case SourceBlock(irGraph) =>
        val graphSource = context.resolver(irGraph.name)
        producer.planStart(ExternalLogicalGraph(irGraph.name, graphSource.canonicalURI, graphSource.schema.get), context.inputRecordFields)
      case x =>
        Raise.notYetImplemented(s"leaf planning of $x")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where, optional, graph) =>
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planMatchPattern(plan, pattern, where, graph)
        if (optional) producer.planOptional(plan, patternPlan) else patternPlan

      case ProjectBlock(_, FieldsAndGraphs(fields, graphs), where, _, distinct) =>
        val withGraphs = planGraphProjections(plan, graphs)
        val withFields = planFieldProjections(withGraphs, fields)
        val filtered = planFilter(withFields, where)
        if (distinct) {
          producer.planDistinct(fields.keySet, filtered)
        } else {
          filtered
        }

      case OrderAndSliceBlock(_, sortItems, skip, limit, _) =>
        val orderOp = if (sortItems.nonEmpty) producer.planOrderBy(sortItems, plan) else plan

        val skipOp = skip match {
          case Some(expr) => producer.planSkip(expr, orderOp)
          case None => orderOp
        }

        limit match {
          case Some(expr) => producer.planLimit(expr, skipOp)
          case None => skipOp
        }

      case AggregationBlock(_, a@Aggregations(pairs), group, _) =>
        // plan projection of aggregation argument
        val prev = pairs.foldLeft(plan)((prevPlan, aggField) => {
          aggField match {
            case (_, agg: Aggregator) => agg.inner.map(e => planInnerExpr(e, prevPlan)).getOrElse(prevPlan)
          }
        })
        producer.aggregate(a, group, prev)

      case x =>
        Raise.notYetImplemented(s"logical planning of $x")
    }
  }

  private def planGraphProjections(in: LogicalOperator, graphs: Set[IRGraph])(implicit context: LogicalPlannerContext): LogicalOperator = {
    graphs.foldLeft(in) {
      case (planSoFar, nextGraph) =>
        val logicalGraph = resolveGraph(nextGraph)
        ProjectGraph(logicalGraph, planSoFar)(planSoFar.solved.withGraph(nextGraph))
    }
  }

  private def planFieldProjections(in: LogicalOperator, exprs: Map[IRField, Expr])(implicit context: LogicalPlannerContext) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) =>
        producer.projectField(f, p, acc)
      case (acc, (f, func: FunctionExpr)) =>
        val projectArg = planInnerExpr(func.expr, acc)
        producer.projectField(f, func, projectArg)
        // this is for aliasing
      case (acc, (f, v: Var)) if f.name != v.name =>
        producer.projectField(f, v, acc)
      case (acc, (_, _: Var)) =>
        acc
      case (acc, (f, be: BinaryExpr)) =>
        val projectLhs = planInnerExpr(be.lhs, acc)
        val projectRhs = planInnerExpr(be.rhs, projectLhs)
        producer.projectField(f, be, projectRhs)
      case (acc, (f, c: Const)) =>
        producer.projectField(f, c, acc)
      case (_, (_, x)) =>
        Raise.notYetImplemented(s"projection of $x")
    }
  }

  // TODO: Should we check (or silently drop) predicates that are not eligible for planning here? (check dependencies)
  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(implicit context: LogicalPlannerContext)
  : LogicalOperator = {
    val filtersAndProjs = where.elements.foldLeft(in) {
      case (acc, ors: Ors) =>
        val withInnerExprs = ors.exprs.foldLeft(acc) {
          case (_acc, expr) => planInnerExpr(expr, _acc)
        }
        producer.planFilter(ors, withInnerExprs)
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
      case (acc, exists@Exists(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(exists, project)
      case (acc, isNull@IsNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNull, acc)
      case (acc, isNotNull@IsNotNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNotNull, acc)
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
      case HasLabel(e,_) => planInnerExpr(e, in)
      case Not(e) => planInnerExpr(e, in)
      case IsNull(e) => planInnerExpr(e, in)
      case IsNotNull(e) => planInnerExpr(e, in)
      case func: FunctionExpr =>
        val projectArg = planInnerExpr(func.expr, in)
        producer.projectExpr(func, projectArg)
      case x =>
        Raise.notYetImplemented(s"projection of inner expression $x")
    }
  }

  // TODO: Add support for pattern graphs that don't have a URI.
  private def resolveGraph(graph: IRGraph)(implicit context: LogicalPlannerContext): LogicalGraph = {
    val graphSource = context.resolver(graph.name)
    val schema = graphSource.schema match {
      case None =>
        // This initialises the graph eagerly!!
        // TODO: We probably want to save the graph reference somewhere
        graphSource.graph.schema
      case Some(s) => s
    }
    ExternalLogicalGraph(graph.name, graphSource.canonicalURI, schema)
  }

  private def planStart(graph: IRGraph)(implicit context: LogicalPlannerContext): Start = {
    val logicalGraph: LogicalGraph = resolveGraph(graph)

    producer.planStart(logicalGraph, context.inputRecordFields)
  }

  private def setSource(graph: IRGraph, prev: LogicalOperator)(implicit context: LogicalPlannerContext): SetSourceGraph = {
    val logicalGraph = resolveGraph(graph)

    producer.planSetSourceGraph(logicalGraph, prev)
  }

  private def planMatchPattern(plan: LogicalOperator, pattern: Pattern[Expr], where: AllGiven[Expr], graph: IRGraph)(implicit context: LogicalPlannerContext) = {
    val components = pattern.components.toSeq
    if (components.size == 1) {
      val patternPlan = planComponentPattern(plan, components.head, graph)
      val filteredPlan = planFilter(patternPlan, where)
      filteredPlan
    } else {
      // TODO: Find a way to feed the same input into all arms of the cartesian product without recomputing it
      val bases = plan +: components.map(_ => Start(plan.sourceGraph, Set.empty)(SolvedQueryModel.empty)).tail
      val plans = bases.zip(components).map {
        case (base, component) =>
          val componentPlan = planComponentPattern(base, component, graph)
          val predicates = where.filter(_.evaluable(componentPlan.fields)).filterNot(componentPlan.solved.predicates)
          val filteredPlan = planFilter(componentPlan, predicates)
          filteredPlan
      }
      val result = plans.reduceOption { (lhs, rhs) =>
        val combinedPlan = producer.planCartesianProduct(lhs, rhs)
        val predicates = where.filter(_.evaluable(combinedPlan.fields)).filterNot(combinedPlan.solved.predicates)
        val filteredPlan = planFilter(combinedPlan, predicates)
        filteredPlan
      }
      result.getOrElse(Raise.invalidOrUnsupportedPattern("empty pattern"))
    }
  }

  private def planComponentPattern(plan: LogicalOperator, pattern: Pattern[Expr], graph: IRGraph)
                                  (implicit context: LogicalPlannerContext): LogicalOperator = {

    // find all unsolved nodes from the pattern
    val nodes = pattern.entities.collect {
      case (f, e: EveryNode) if f.cypherType.subTypeOf(CTNode).isTrue => f -> e
    }

    if (nodes.size == 1) { // simple node scan; just do it
      val (field, node) = nodes.head

      // we set the source graph because we don't know what the source graph was coming in
      // TODO: Only works for initial pattern: need support for cartesian products
      nodePlan(setSource(graph, plan), field, node)
    } else if (pattern.topology.nonEmpty) { // we need expansions to tie node plans together

      val solved = nodes.filter(node => plan.solved.fields.contains(node._1))
      val unsolved = nodes -- solved.keySet

      val (firstPlan, remaining) = if (solved.isEmpty) {
        val (field, node) = nodes.head
        // TODO: Will need branch in plan for cartesian products
        nodePlan(plan, field, node) -> nodes.tail
      } else {
        plan -> unsolved
      }

      val nodePlans: Set[LogicalOperator] = remaining.map {
        case (f, n) =>
          nodePlan(planStart(graph), f, n)
      }.toSet

      // tie all plans together using expansions
      planExpansions(nodePlans + firstPlan, pattern, producer)
    } else
      Raise.invalidOrUnsupportedPattern(pattern.toString)
  }

  @tailrec
  private def planExpansions(disconnectedPlans: Set[LogicalOperator], pattern: Pattern[Expr],
                             producer: LogicalOperatorProducer): LogicalOperator = {
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
      case _ if sourcePlan == targetPlan =>
        producer.planExpandInto(c.source, r, pattern.rels(r), c.target, sourcePlan)
      case _ =>
        producer.planSourceExpand(c.source, r, pattern.rels(r), c.target, sourcePlan, targetPlan)
    }

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern, producer)
  }

  private def nodePlan(plan: LogicalOperator, field: IRField, everyNode: EveryNode)(implicit context: LogicalPlannerContext) = {
    producer.planNodeScan(field, everyNode, plan)
  }
}
