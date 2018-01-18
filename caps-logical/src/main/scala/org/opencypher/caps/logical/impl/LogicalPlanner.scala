/*
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
package org.opencypher.caps.logical.impl

import org.opencypher.caps.api.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.caps.api.schema.{AllGiven, Schema}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block._
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.pattern.{Connection, Pattern, VarLengthRelationship}
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.ir.impl.syntax.ExprSyntax._
import org.opencypher.caps.ir.impl.util.VarConverters._
import org.opencypher.caps.logical.api.exception.{
  InvalidCypherTypeException,
  InvalidDependencyException,
  InvalidPatternException
}

import scala.annotation.tailrec

class LogicalPlanner(producer: LogicalOperatorProducer)
    extends DirectCompilationStage[CypherQuery[Expr], LogicalOperator, LogicalPlannerContext] {

  override def process(ir: CypherQuery[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val model = ir.model

    planModel(model.result, model)
  }

  def planModel(block: ResultBlock[Expr], model: QueryModel[Expr])(
      implicit context: LogicalPlannerContext): LogicalOperator = {
    val first = block.after.head // there should only be one, right?
    val plan = planBlock(first, model, None)

    // always plan a select at the top
    val fields = block.binds.fieldsOrder.map(f => Var(f.name)(f.cypherType))
    val graphNames = block.binds.graphs.map(_.name)
    producer.planSelect(fields, graphNames, plan)
  }

  final def planBlock(ref: BlockRef, model: QueryModel[Expr], plan: Option[LogicalOperator])(
      implicit context: LogicalPlannerContext): LogicalOperator = {
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
          // TODO: refactor to remove illegal state
          block.after
            .find(r => !_plan.solved.contains(model(r)))
            .getOrElse(throw IllegalStateException("Found block with unsolved dependencies which cannot be solved."))
      }
      val dependency = planBlock(depRef, model, plan)
      planBlock(ref, model, Some(dependency))
    }
  }

  def planLeaf(ref: BlockRef, model: QueryModel[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case SourceBlock(irGraph) =>
        val graphSource = context.resolver(irGraph.name)
        producer.planStart(
          LogicalExternalGraph(irGraph.name, graphSource.canonicalURI, graphSource.schema.get),
          context.inputRecordFields)
      case x =>
        throw NotImplementedException(s"Support for leaf planning of $x not yet implemented")
    }
  }

  def planNonLeaf(ref: BlockRef, model: QueryModel[Expr], plan: LogicalOperator)(
      implicit context: LogicalPlannerContext): LogicalOperator = {
    model(ref) match {
      case MatchBlock(_, pattern, where, optional, graph) =>
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planMatchPattern(plan, pattern, where, graph)(context.withSourceGraph(graph))
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
          case None       => orderOp
        }

        limit match {
          case Some(expr) => producer.planLimit(expr, skipOp)
          case None       => skipOp
        }

      case AggregationBlock(_, a @ Aggregations(pairs), group, _) =>
        // plan projection of aggregation argument
        val prev = pairs.foldLeft(plan) {
          case (prevPlan, (_, agg)) =>
            agg match {
              case a: Aggregator => a.inner.map(e => planInnerExpr(e, prevPlan)).getOrElse(prevPlan)
              case _             => throw IllegalArgumentException("an aggregator", agg)
            }
        }
        producer.aggregate(a, group, prev)

      case UnwindBlock(_, UnwoundList(list, variable), _) =>
        val withList = planInnerExpr(list, plan)
        producer.planUnwind(list, variable, withList)

      case x =>
        throw NotImplementedException(s"Support for logical planning of $x not yet implemented")
    }
  }

  private def planGraphProjections(in: LogicalOperator, graphs: Set[IRGraph])(
      implicit context: LogicalPlannerContext): LogicalOperator = {
    val graphsToProject = graphs.filterNot(in.solved.solves)

    graphsToProject.foldLeft(in) {
      case (planSoFar, nextGraph) =>
        val logicalGraph = resolveGraph(nextGraph, in.sourceGraph.schema, in.fields)
        ProjectGraph(logicalGraph, planSoFar, planSoFar.solved.withGraph(nextGraph.toNamedGraph))
    }
  }

  private def planFieldProjections(in: LogicalOperator, exprs: Map[IRField, Expr])(
      implicit context: LogicalPlannerContext) = {
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
      case (acc, (f, c: Param)) =>
        producer.projectField(f, c, acc)
      case (acc, (f, c: Lit[_])) =>
        producer.projectField(f, c, acc)
      case (_, (_, x)) =>
        throw NotImplementedException(s"Support for projection of $x not yet implemented")
    }
  }

  // TODO: Should we check (or silently drop) predicates that are not eligible for planning here? (check dependencies)
  private def planFilter(in: LogicalOperator, where: AllGiven[Expr])(
      implicit context: LogicalPlannerContext): LogicalOperator = {
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
        val projectParent = producer.projectExpr(be, project2)
        producer.planFilter(be, projectParent)

      case (acc, h @ HasLabel(_: Var, _)) =>
        producer.planFilter(h, acc)

      case (acc, not @ Not(Equals(lhs, rhs))) =>
        val p1 = planInnerExpr(lhs, acc)
        val p2 = planInnerExpr(rhs, p1)
        producer.planFilter(not, p2)

      case (acc, not @ Not(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(not, project)

      case (acc, exists @ Exists(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(exists, project)

      case (acc, isNull @ IsNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNull, acc)

      case (acc, isNotNull @ IsNotNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNotNull, acc)

      case (acc, t: TrueLit) =>
        producer.planFilter(t, acc) // optimise away this one somehow... currently we do that in PhysicalPlanner

      case (acc, v: Var) =>
        producer.planFilter(v, acc)

      case (acc, ex: ExistsPatternExpr) =>
        val innerPlan = this(ex.ir)
        val predicate = producer.planExistsPatternPredicate(ex, acc, innerPlan)
        producer.planFilter(ex, predicate)

      case (_, x) =>
        throw NotImplementedException(s"Support for logical planning of predicate $x not yet implemented")
    }

    filtersAndProjs
  }

  private def planInnerExpr(expr: Expr, in: LogicalOperator)(
      implicit context: LogicalPlannerContext): LogicalOperator = {
    expr match {
      case _: Param => in

      case _: Lit[_] => in

      case _: Var => in

      case p: Property =>
        producer.projectExpr(p, in)

      case be: BinaryExpr =>
        val project1 = planInnerExpr(be.lhs, in)
        val project2 = planInnerExpr(be.rhs, project1)
        producer.projectExpr(be, project2)

      case HasLabel(e, _) => planInnerExpr(e, in)

      case Not(e) => planInnerExpr(e, in)

      case IsNull(e) => planInnerExpr(e, in)

      case IsNotNull(e) => planInnerExpr(e, in)

      case func: FunctionExpr =>
        val projectArg = planInnerExpr(func.expr, in)
        producer.projectExpr(func, projectArg)

      case ex: ExistsPatternExpr =>
        val innerPlan = this(ex.ir)
        producer.planExistsPatternPredicate(ex, in, innerPlan)

      case x =>
        throw NotImplementedException(s"Support for projection of inner expression $x not yet implemented")
    }
  }

  private def resolveGraph(graph: IRGraph, sourceSchema: Schema, fieldsInScope: Set[Var])(
      implicit context: LogicalPlannerContext): LogicalGraph = {

    graph match {
      // TODO: IRGraph[Expr]
      case IRPatternGraph(name, schema, pattern) =>
        val patternEntities = pattern.fields
        val entitiesInScope = fieldsInScope.map { (v: Var) =>
          IRField(v.name)(v.cypherType)
        }
        val boundEntities = patternEntities intersect entitiesInScope
        val entitiesToCreate = patternEntities -- boundEntities

        val entities: Set[ConstructedEntity] = entitiesToCreate.map { e =>
          e.cypherType match {
            case CTRelationship(relTypes) if relTypes.size == 1 =>
              val connection = pattern.topology(e)
              ConstructedRelationship(e, connection.source, connection.target, relTypes.head)
            case CTNode(labels) =>
              ConstructedNode(e, labels.map(Label))
            case _ =>
              throw InvalidCypherTypeException(s"Expected an entity type (CTNode, CTRelationShip), got $e")
          }
        }

        LogicalPatternGraph(name, schema, GraphOfPattern(entities, boundEntities))

      case _ =>
        val graphSource = context.resolver(graph.name)
        val schema = graphSource.schema match {
          case None =>
            // This initialises the graph eagerly!!
            // TODO: We probably want to save the graph reference somewhere
            graphSource.graph.schema
          case Some(s) => s
        }
        LogicalExternalGraph(graph.name, graphSource.canonicalURI, schema)
    }
  }

  private def planStart(graph: IRGraph)(implicit context: LogicalPlannerContext): Start = {
    val logicalGraph: LogicalGraph = resolveGraph(graph, Schema.empty, Set.empty)

    producer.planStart(logicalGraph, context.inputRecordFields)
  }

  private def planSetSourceGraph(graph: IRGraph, prev: LogicalOperator)(
      implicit context: LogicalPlannerContext): SetSourceGraph = {
    val logicalGraph = resolveGraph(graph, prev.sourceGraph.schema, prev.fields)

    producer.planSetSourceGraph(logicalGraph, prev)
  }

  private def planMatchPattern(plan: LogicalOperator, pattern: Pattern[Expr], where: AllGiven[Expr], graph: IRGraph)(
      implicit context: LogicalPlannerContext) = {
    val components = pattern.components.toSeq
    if (components.size == 1) {
      val patternPlan = planComponentPattern(plan, components.head, graph)
      val filteredPlan = planFilter(patternPlan, where)
      filteredPlan
    } else {
      // TODO: Find a way to feed the same input into all arms of the cartesian product without recomputing it
      val bases = plan +: components.map(_ => Start(plan.sourceGraph, Set.empty, SolvedQueryModel.empty)).tail
      val plans = bases.zip(components).map {
        case (base, component) =>
          val componentPlan = planComponentPattern(base, component, graph)
          val predicates = where.filter(_.evaluable(componentPlan.fields)).filterNot(componentPlan.solved.predicates)
          val filteredPlan = planFilter(componentPlan, predicates)
          filteredPlan
      }
      val result = plans.reduceOption { (lhs, rhs) =>
        val fieldsInScope = lhs.fields ++ rhs.fields
        val solvedPredicates = lhs.solved.predicates ++ rhs.solved.predicates
        val predicates = where.filter(_.evaluable(fieldsInScope)).filterNot(solvedPredicates)
        val (joinPredicates, otherPredicates) = predicates.flatPartition {
          case expr: org.opencypher.caps.ir.api.expr.Equals => expr
        }
        if (joinPredicates.isEmpty) {
          val combinedPlan = producer.planCartesianProduct(lhs, rhs)
          val filteredPlan = planFilter(combinedPlan, predicates)
          filteredPlan
        } else {
          val (leftIn, rightIn) = joinPredicates.elements.foldLeft((lhs, rhs)) {
            case ((l, r), predicate) => producer.projectExpr(predicate.lhs, l) -> producer.projectExpr(predicate.rhs, r)
          }
          producer.planValueJoin(leftIn, rightIn, joinPredicates.elements)
        }
      }
      // TODO: use type system to avoid empty pattern
      result.getOrElse(throw InvalidPatternException("Cannot plan an empty match pattern"))
    }
  }

  private def planComponentPattern(plan: LogicalOperator, pattern: Pattern[Expr], graph: IRGraph)(
      implicit context: LogicalPlannerContext): LogicalOperator = {

    // find all unsolved nodes from the pattern
    val nodes = pattern.fields.filter(_.cypherType.subTypeOf(CTNode).isTrue)

    if (pattern.topology.isEmpty) { // there is no connection in the pattern => plan a node scan
      val field = nodes.head

      // if we have already planned a node: cartesian product
      if (plan.solved.fields.exists(_.cypherType.subTypeOf(CTNode).isTrue)) {
        producer.planCartesianProduct(plan, nodePlan(planSetSourceGraph(graph, planStart(graph)), field))
      } else { // first node scan
        // we set the source graph because we don't know what the source graph was coming in
        nodePlan(planSetSourceGraph(graph, plan), field)
      }
    } else { // there are connections => we need to expand

      val solved = nodes.intersect(plan.solved.fields)
      val unsolved = nodes -- solved

      val (firstPlan, remaining) = if (solved.isEmpty) {
        val field = nodes.head
        // TODO: Will need branch in plan for cartesian products
        nodePlan(planSetSourceGraph(graph, plan), field) -> nodes.tail
      } else {
        plan -> unsolved
      }

      val nodePlans: Set[LogicalOperator] = remaining.map {
        nodePlan(planStart(graph), _)
      }.toSet

      // tie all plans together using expansions
      planExpansions(nodePlans + firstPlan, pattern, producer)
    }
  }

  @tailrec
  private def planExpansions(
      disconnectedPlans: Set[LogicalOperator],
      pattern: Pattern[Expr],
      producer: LogicalOperatorProducer): LogicalOperator = {
    val allSolved = disconnectedPlans.map(_.solved).reduce(_ ++ _)

    val (r, c) = pattern.topology.collectFirst {
      case (rel, conn: Connection) if !allSolved.solves(rel) =>
        rel -> conn
    }.getOrElse(
      // TODO: exclude case with type system
      throw InvalidPatternException("Cannot plan an expansion that has already been solved")
    )

    val sourcePlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.source) => p
    }.getOrElse(throw InvalidDependencyException("Cannot plan expansion for unsolved source plan"))
    val targetPlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.target) => p
    }.getOrElse(throw InvalidDependencyException("Cannot plan expansion for unsolved target plan"))

    val expand = c match {
      case v: VarLengthRelationship if v.upper.nonEmpty =>
        producer.planBoundedVarLengthExpand(c.source, r, c.target, v.lower, v.upper.get, sourcePlan, targetPlan)
      case _ if sourcePlan == targetPlan =>
        producer.planExpandInto(c.source, r, c.target, sourcePlan)
      case _ =>
        producer.planSourceExpand(c.source, r, c.target, sourcePlan, targetPlan)
    }

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern, producer)
  }

  private def nodePlan(plan: LogicalOperator, field: IRField)(implicit context: LogicalPlannerContext) = {
    producer.planNodeScan(field, plan)
  }
}
