/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.ir.api.{Label, _}
import org.opencypher.okapi.ir.impl.syntax.ExprSyntax._
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl.exception.{InvalidCypherTypeException, InvalidDependencyException, InvalidPatternException}
import org.opencypher.v9_0.expressions.SemanticDirection.{INCOMING, OUTGOING}

import scala.annotation.tailrec

class LogicalPlanner(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[CypherQuery, LogicalOperator, LogicalPlannerContext] {

  override def process(ir: CypherQuery)(implicit context: LogicalPlannerContext): LogicalOperator = {
    ir match {
      case sq: SingleQuery => planModel(sq.model.result, sq.model)
      case UnionQuery(left, right, distinct) =>
        val leftOperator = process(left)
        val rightOperator = process(right)

        val isLeftGraph = leftOperator match {
          case _: ReturnGraph | _: GraphUnionAll => true
          case _ => false
        }

        val isRightGraph = rightOperator match {
          case _: ReturnGraph | _: GraphUnionAll => true
          case _ => false
        }

        (isLeftGraph, isRightGraph) match {
          case (true, true) =>
            if (distinct) throw UnsupportedOperationException("Distinct Union between graphs")
            else GraphUnionAll(leftOperator, rightOperator)
          case (false, false) =>
            val union = TabularUnionAll(leftOperator, rightOperator)
            if (distinct) Distinct(union.fields, union, union.solved) else union
          case _ => throw UnsupportedOperationException("Union between graph and table is not supported")
        }
    }
  }

  def planModel(block: ResultBlock, model: QueryModel)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    val first = block.after.head // there should only be one, right?

    val plan = planBlock(first, model, None)

    // always plan a select at the top
    block match {
      case t: TableResultBlock =>
        val fields = t.binds.orderedFields.map(f => Var(f.name)(f.cypherType))
        producer.planSelect(fields, plan)
      case g: GraphResultBlock =>
        producer.planReturnGraph(producer.planFromGraph(resolveGraph(g.graph, plan.fields), plan))
    }
  }

  final def planBlock(block: Block, model: QueryModel, plan: Option[LogicalOperator])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    if (block.after.isEmpty) {
      // this is a leaf block, just plan it
      planLeaf(block, model)
    } else if (plan.nonEmpty && plan.get.solved.contains(block.after.toSet)) {
      // all deps satisfied for this block, we can just plan it if we have already planned a leaf
      planNonLeaf(block, model, plan.get)
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
            .find(r => !_plan.solved.contains(r))
            .getOrElse(throw IllegalStateException("Found block with unsolved dependencies which cannot be solved."))
      }
      val dependency = planBlock(depRef, model, plan)
      planBlock(block, model, Some(dependency))
    }
  }

  def planLeaf(block: Block, model: QueryModel)(implicit context: LogicalPlannerContext): LogicalOperator = {
    block match {
      case SourceBlock(irGraph: IRCatalogGraph) =>
        val qualifiedGraphName = irGraph.qualifiedGraphName
        val logicalGraph = LogicalCatalogGraph(qualifiedGraphName, context.resolveSchema(qualifiedGraphName))

        if (context.inputRecordFields.isEmpty) {
          producer.planStart(logicalGraph)
        } else {
          producer.planStartWithDrivingTable(logicalGraph, context.inputRecordFields)
        }
      case x =>
        throw NotImplementedException(s"Support for leaf planning of $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  def planNonLeaf(block: Block, model: QueryModel, plan: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    block match {
      case MatchBlock(_, pattern, where, optional, graph) =>
        val lg = resolveGraph(graph, plan.fields)
        val inputGraphPlan = if (plan.graph == lg) {
          plan
        } else {
          plan match {
            // If the inner plan is a start, simply rewrite it to start with the required graph
            case Start(_, solved) => Start(lg, solved)
            case _ => planFromGraph(lg, plan)
          }
        }
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planMatchPattern(inputGraphPlan, pattern, where, graph)
        if (optional) producer.planOptional(inputGraphPlan, patternPlan) else patternPlan

      case ProjectBlock(_, Fields(fields), where, _, distinct) =>
        val withFields = planFieldProjections(plan, fields)
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

      case AggregationBlock(_, a, group, _) =>
        producer.aggregate(a, group, plan)

      case UnwindBlock(_, UnwoundList(list, variable), _) =>
        producer.planUnwind(list, variable, plan)

      case GraphResultBlock(_, graph) =>
        producer.planReturnGraph(producer.planFromGraph(resolveGraph(graph, plan.fields), plan))

      case x =>
        throw NotImplementedException(s"Support for logical planning of $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  private def planFieldProjections(in: LogicalOperator, exprs: Map[IRField, Expr])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    exprs.foldLeft(in) {

      case (acc, (f, ex: ExistsPatternExpr)) =>
        val existsPlan = producer.planExistsSubQuery(ex, acc, this (ex.ir))
        producer.projectField(existsPlan.expr.targetField, f, existsPlan)

      case (acc, (f, expr)) =>
        if (acc.solved.solves(f)) {
          acc
        } else {
          producer.projectField(expr, f, expr.children.foldLeft(acc)((op, e) => planInnerSubquery(e, op)))
        }

    }
  }

  private def planFilter(in: LogicalOperator, where: Set[Expr])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    val filtersAndProjections = where.foldLeft(in) {

      case (acc, ex: ExistsPatternExpr) =>
        val predicate = producer.planExistsSubQuery(ex, acc, this (ex.ir))
        producer.planFilter(ex, predicate)

      case (acc, predicate) =>
        val withInnerExpressions = predicate.children.foldLeft(acc)((acc, e) => planInnerSubquery(e, acc))
        producer.planFilter(predicate, withInnerExpressions)

    }

    filtersAndProjections
  }

  private def planInnerSubquery(expr: Expr, in: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    expr match {

      case ex: ExistsPatternExpr =>
        producer.planExistsSubQuery(ex, in, this (ex.ir))

      case _ =>
        expr.children.foldLeft(in)((op, e) => planInnerSubquery(e, op))

    }
  }

  private def resolveGraph(graph: IRGraph, fieldsInScope: Set[Var])(
    implicit context: LogicalPlannerContext
  ): LogicalGraph = {

    graph match {
      // TODO: IRGraph
      case p: IRPatternGraph =>
        import org.opencypher.okapi.ir.impl.util.VarConverters.RichIrField
        val baseElements = p.creates.baseFields.mapValues(_.toVar)

        val clonePatternElements = p.clones.keys

        val newPatternElements = p.creates.fields

        val elementsToCreate = newPatternElements -- clonePatternElements

        val clonedVarToInputVar: Map[Var, Var] = p.clones.map { case (clonedField, inputExpression) =>
          val inputVar = inputExpression match {
            case v: Var => v
            case other => throw IllegalArgumentException("CLONED expression to be a variable", other)
          }
          clonedField.toVar -> inputVar
        }

        val newElements: Set[ConstructedElement] = elementsToCreate.map(e => extractConstructedElements(p.creates, e, baseElements.get(e)))

        val setItems = {
          val setPropertyItemsFromCreates = p.creates.properties.flatMap { case (irField, mapExpr) =>
            val v = irField.toVar
            mapExpr.items.map { case (propertyKey, expr) =>
              SetPropertyItem(propertyKey, v, expr)
            }
          }
          setPropertyItemsFromCreates ++ p.sets
        }.toList

        LogicalPatternGraph(p.schema, clonedVarToInputVar, newElements, setItems, p.onGraphs, p.qualifiedGraphName)

      case IRCatalogGraph(qgn, schema) => LogicalCatalogGraph(qgn, schema)

    }
  }

  private def extractConstructedElements(pattern: Pattern, e: IRField, baseField: Option[Var]) = e.cypherType match {
    case CTRelationship(relTypes, _) if relTypes.size <= 1 =>
      val connection = pattern.topology(e)
      ConstructedRelationship(e, connection.source, connection.target, relTypes.headOption, baseField)
    case CTNode(labels, _) =>
      ConstructedNode(e, labels.map(Label), baseField)
    case other =>
      throw InvalidCypherTypeException(s"Expected an element type (CTNode, CTRelationship), got $other")
  }

  private def planStart(graph: IRGraph)(implicit context: LogicalPlannerContext): Start = {
    val logicalGraph: LogicalGraph = resolveGraph(graph, Set.empty)

    producer.planStart(logicalGraph)
  }

  private def planFromGraph(graph: LogicalGraph, prev: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): FromGraph = {

    producer.planFromGraph(graph, prev)
  }

  private def planMatchPattern(plan: LogicalOperator, pattern: Pattern, where: Set[Expr], graph: IRGraph)(
    implicit context: LogicalPlannerContext
  ) = {
    val components = pattern.components.toSeq
    if (components.size == 1) {
      val patternPlan = planComponentPattern(plan, components.head, graph)
      val filteredPlan = planFilter(patternPlan, where)
      filteredPlan
    } else {
      components.foldLeft(plan) {
        case (base, component) =>
          val componentPlan = planComponentPattern(base, component, graph)
          val predicates = where.filter(_.canEvaluate(componentPlan.fields)).filterNot(componentPlan.solved.predicates)
          val filteredPlan = planFilter(componentPlan, predicates)
          filteredPlan
      }
    }
  }

  private def planComponentPattern(plan: LogicalOperator, pattern: Pattern, graph: IRGraph)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {

    // find all unsolved nodes from the pattern
    val nodes = pattern.fields.filter(_.cypherType.subTypeOf(CTNode))

    if (pattern.topology.isEmpty) { // there is no connection in the pattern => plan a node scan
      val field = nodes.head

      // if we have already have a previous result we need to plan a cartesian product
      if (plan.fields.nonEmpty) {
        producer.planCartesianProduct(plan, nodePlan(planStart(graph), field))
      } else { // first node scan
        nodePlan(plan, field)
      }
    } else { // there are connections => we need to expand

      val solved = nodes.intersect(plan.solved.fields)
      val unsolved = nodes -- solved

      val (firstPlan, remaining) = if (solved.isEmpty) { // there is no connection to the previous plan
        val field = nodes.head
        if (plan.fields.nonEmpty) { // there are already fields in the previous plan, we need to plan a cartesian product
          producer.planCartesianProduct(plan, nodePlan(planStart(graph), field)) -> nodes.tail
        } else { // there are no previous results, it's safe to plan a node scan
          nodePlan(plan, field) -> nodes.tail
        }
      } else { // we can connect to the previous plan
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
    pattern: Pattern,
    producer: LogicalOperatorProducer
  ): LogicalOperator = {
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
        val direction = v match {
          case rel: DirectedVarLengthRelationship if rel.semanticDirection == OUTGOING => Outgoing
          case rel: DirectedVarLengthRelationship if rel.semanticDirection == INCOMING => Incoming
          case _: UndirectedVarLengthRelationship => Undirected
        }

        if (v.upper.getOrElse(Integer.MAX_VALUE) < v.lower) {
          val solved = sourcePlan.solved ++ targetPlan.solved.withField(r)
          EmptyRecords(sourcePlan.fields ++ targetPlan.fields, Start(targetPlan.graph, solved), solved)
        } else {
          producer.planBoundedVarLengthExpand(c.source, r, c.target, v.edgeType, direction, v.lower, v.upper.get, sourcePlan, targetPlan)
        }

      case _: UndirectedConnection if sourcePlan == targetPlan =>
        producer.planExpandInto(c.source, r, c.target, Undirected, sourcePlan)
      // cyclic and directed are handled the same way for expandInto

      case _: CyclicRelationship =>
        producer.planExpandInto(c.source, r, c.target, Outgoing, sourcePlan)

      case rel: DirectedRelationship if sourcePlan == targetPlan && rel.semanticDirection == OUTGOING =>
        producer.planExpandInto(c.source, r, c.target, Outgoing, sourcePlan)

      case rel: DirectedRelationship if sourcePlan == targetPlan && rel.semanticDirection == INCOMING =>
        producer.planExpandInto(c.source, r, c.target, Incoming, sourcePlan)

      case rel: DirectedRelationship if rel.semanticDirection == OUTGOING =>
        producer.planExpand(c.source, r, c.target, Outgoing, sourcePlan, targetPlan)

      case rel: DirectedRelationship if rel.semanticDirection == INCOMING =>
        producer.planExpand(c.source, r, c.target, Incoming, sourcePlan, targetPlan)

      case _: UndirectedConnection =>
        producer.planExpand(c.source, r, c.target, Undirected, sourcePlan, targetPlan)

    }

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern, producer)
  }

  private def nodePlan(plan: LogicalOperator, field: IRField)(implicit context: LogicalPlannerContext) = {
    producer.planNodeScan(field, plan)
  }
}
