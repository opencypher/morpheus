/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.io.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.impl.flat
import org.opencypher.okapi.relational.impl.flat.FlatOperator

class PhysicalPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]](val producer: PhysicalOperatorProducer[O, K, A, P, I])

  extends DirectCompilationStage[FlatOperator, K, PhysicalPlannerContext[O, K, A]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[O, K, A]): K = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs) =>
        producer.planCartesianProduct(process(lhs), process(rhs))

      case flat.Select(fields, in) =>

        val inOp = process(in)

        val selectExpressions = fields
          .flatMap(inOp.header.ownedBy)
          .distinct

        producer.planSelect(inOp, selectExpressions)

      case flat.Project(projectExpr, in) =>
        val inOp = process(in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.header.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => producer.planAlias(inOp, expr as alias)
          case Some(alias) => producer.planAddColumn(inOp, expr as alias)
          case None => producer.planAddColumn(inOp, expr)
        }

      case flat.EmptyRecords(in, fields) =>
        producer.planEmptyRecords(process(in), fields)

      case flat.Start(graph) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planStart(Some(g.qualifiedGraphName), Some(context.inputRecords))
          case p: LogicalPatternGraph =>
            context.constructedGraphPlans.get(p.name) match {
              case Some(plan) => plan // the graph was already constructed
              case None => planConstructGraph(None, p) // plan starts with a construct graph, thus we have to plan it
            }
        }

      case flat.FromGraph(graph, in) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planFromGraph(process(in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct)
        }

      case op
        @flat.NodeScan(v, in) => producer.planNodeScan(process(in), op.sourceGraph, v)

      case op
        @flat.RelationshipScan(e, in) => producer.planRelationshipScan(process(in), op.sourceGraph, e)

      case flat.Alias(expr, in) => producer.planAlias(process(in), expr)

      case flat.WithColumn(expr, in) =>
        producer.planAddColumn(process(in), expr)

      case flat.Aggregate(aggregations, group, in) => producer.planAggregate(process(in), group, aggregations)

      case flat.Filter(expr, in) => expr match {
        case TrueLit =>
          process(in) // optimise away filter
        case _ =>
          producer.planFilter(process(in), expr)
      }

      case flat.ValueJoin(lhs, rhs, predicates) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions)

      case flat.Distinct(fields, in) =>
        val entityExprs: Set[Var] = Set(fields.toSeq: _*)
        producer.planDistinct(process(in), entityExprs)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val startFrom = sourceOp.sourceGraph match {
          case e: LogicalCatalogGraph =>
            producer.planStart(Some(e.qualifiedGraphName))

          case c: LogicalPatternGraph =>
            context.constructedGraphPlans(c.name)
        }

        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = producer.planJoin(first, second, Seq(source -> startNode))
            producer.planJoin(tempResult, third, Seq(endNode -> target))

          case Undirected =>
            val tempOutgoing = producer.planJoin(first, second, Seq(source -> startNode))
            val outgoing = producer.planJoin(tempOutgoing, third, Seq(endNode -> target))

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = producer.planFilter(second, filterExpression)

            val tempIncoming = producer.planJoin(third, relsWithoutLoops, Seq(target -> startNode))
            val incoming = producer.planJoin(tempIncoming, first, Seq(endNode -> source))

            producer.planTabularUnionAll(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode))

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode))
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode))
            producer.planTabularUnionAll(outgoing, incoming)
        }

      case flat.BoundedVarExpand(
      source, list, edgeScan, target,
      direction, lower, upper,
      sourceOp, edgeScanOp, targetOp,
      isExpandInto
      ) =>
        val planner = direction match {
          case Directed => new DirectedVarLengthExpandPlanner[O, K, A, P, I](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)(this, context)

          case Undirected => new UndirectedVarLengthExpandPlanner[O, K, A, P, I](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)(this, context)
        }

        planner.plan

      case flat.Optional(lhs, rhs) => planOptional(lhs, rhs)

      case flat.ExistsSubQuery(predicateField, lhs, rhs) =>

        val leftResult = process(lhs)
        val rightResult = process(rhs)

        val leftHeader = leftResult.header
        val rightHeader = rightResult.header

        // 0. Find common expressions, i.e. join expressions
        val joinExprs = leftHeader.vars.intersect(rightHeader.vars)
        // 1. Alias join expressions on rhs
        val renameExprs = joinExprs.map(e => e as Var(s"${e.name}${System.nanoTime}")(e.cypherType))
        val rightWithAliases = producer.planAliases(rightResult, renameExprs.toSeq)
        // 2. Drop Join expressions and their children in rhs
        val epxrsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = producer.planDrop(rightWithAliases, epxrsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = producer.planDistinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = producer.planJoin(leftResult, distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        producer.planCopyColumn(joinedData, IsNotNull(targetExpr)(CTBoolean), predicateField)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in) =>
        producer.planOrderBy(process(in), sortItems)

      case flat.Skip(expr, in) => producer.planSkip(process(in), expr)

      case flat.Limit(expr, in) => producer.planLimit(process(in), expr)

      case flat.ReturnGraph(in) => producer.planReturnGraph(process(in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  private def planConstructGraph(in: Option[FlatOperator], construct: LogicalPatternGraph)
    (implicit context: PhysicalPlannerContext[O, K, A]) = {
    val onGraphPlan = {
      construct.onGraphs match {
        case Nil => producer.planStart() // Empty start
        //TODO: Optimize case where no union is necessary
        //case h :: Nil => producer.planStart(Some(h)) // Just one graph, no union required
        case several =>
          val onGraphPlans = several.map(qgn => producer.planStart(Some(qgn)))
          producer.planGraphUnionAll(onGraphPlans, construct.name)
      }
    }
    val inputTablePlan = in.map(process).getOrElse(producer.planStart())

    val constructGraphPlan = producer.planConstructGraph(inputTablePlan, onGraphPlan, construct)
    context.constructedGraphPlans.update(construct.name, constructGraphPlan)
    constructGraphPlan
  }

  private def planOptional(lhs: FlatOperator, rhs: FlatOperator)
    (implicit context: PhysicalPlannerContext[O, K, A]) = {
    val lhsOp = process(lhs)
    val rhsOp = process(rhs)

    val lhsHeader = lhsOp.header
    val rhsHeader = rhsOp.header

    def generateUniqueName = s"tmp${System.nanoTime}"

    // 1. Compute expressions between left and right side
    val commonExpressions = lhsHeader.expressions.intersect(rhsHeader.expressions)
    val joinExprs = commonExpressions.collect { case v: Var => v }
    val otherExpressions = commonExpressions -- joinExprs

    // 2. Remove siblings of the join expressions and other common fields
    val expressionsToRemove = joinExprs
      .flatMap(v => rhsHeader.ownedBy(v) - v)
      .union(otherExpressions)
    val rhsWithDropped = producer.planDrop(rhsOp, expressionsToRemove)

    // 3. Rename the join expressions on the right hand side, in order to make them distinguishable after the join
    val joinExprRenames = joinExprs.map(e => e as Var(generateUniqueName)(e.cypherType))
    val rhsWithAlias = producer.planAliases(rhsWithDropped, joinExprRenames.toSeq)
    val rhsJoinReady = producer.planDrop(rhsWithAlias, joinExprs.collect { case e: Expr => e })

    // 4. Left outer join the left side and the processed right side
    val joined = producer.planJoin(lhsOp, rhsJoinReady, joinExprRenames.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)

    // 5. Select the resulting header expressions
    producer.planSelect(joined, joined.header.expressions.toList)
  }
}
