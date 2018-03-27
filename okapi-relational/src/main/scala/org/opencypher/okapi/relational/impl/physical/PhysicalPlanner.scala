/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.impl.schema.TagSupport
import org.opencypher.okapi.impl.schema.TagSupport._
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.impl.flat
import org.opencypher.okapi.relational.impl.flat.FlatOperator

class PhysicalPlanner[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]](producer: PhysicalOperatorProducer[P, R, G, C])

  extends DirectCompilationStage[FlatOperator, P, PhysicalPlannerContext[R]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[R]): P = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        producer.planCartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        producer.planRemoveAliases(process(in), dependent, header)

      case flat.Select(fields, in, header) =>
        producer.planSelectFields(process(in), fields, header)

      case flat.EmptyRecords(in, header) =>
        producer.planEmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planStart(Some(context.inputRecords), Some(g.qualifiedGraphName))
          case _ => throw IllegalArgumentException("Start needs a catalog graph", graph)
        }

      case flat.UseGraph(graph, in) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planUseGraph(process(in), g)

          case construct: LogicalPatternGraph =>
            def catalogGraph(qgn: QualifiedGraphName) = producer.planStart(None, Some(qgn))

            def computeRetaggings(graphs: Map[QualifiedGraphName, Schema with TagSupport]): Map[QualifiedGraphName, Map[Int, Int]] = {
              val (result, _) = graphs.foldLeft((Map.empty[QualifiedGraphName, Map[Int, Int]], Set.empty[Int])) {
                case ((graphReplacements, previousTags), (graphId, schema)) =>
                  val rightTags = schema.tags

                  val replacements = previousTags.replacementsFor(rightTags)
                  val updatedRightTags = rightTags.replaceWith(replacements)

                  val updatedPreviousTags = previousTags ++ updatedRightTags
                  val updatedGraphReplacements = graphReplacements.updated(graphId, replacements)

                  updatedGraphReplacements -> updatedPreviousTags
              }
              result
            }

            val varGraphs: Map[Var, QualifiedGraphName] = construct.clones.mapValues(_.cypherType.graph.get)
            val onGraph: Set[QualifiedGraphName] = construct.onGraphs.toSet

            val sourceGraphs = onGraph ++ varGraphs.values.toSet

            if (sourceGraphs.isEmpty) {
              producer.planConstructGraph(process(in), construct, Map.empty.withDefaultValue(Map.empty))
            } else if (sourceGraphs.size == 1) {
              val constructGraphOperator = producer.planConstructGraph(process(in), construct, Map.empty.withDefaultValue(Map.empty))
              if (construct.onGraphs.isEmpty) {
                constructGraphOperator
              } else { // Can be at most one
                val constructOnGraph = catalogGraph(construct.onGraphs.head)
                producer.planGraphUnionAll(List(constructOnGraph, constructGraphOperator))
              }
            } else {
              val schemasForGraphs: Map[QualifiedGraphName, Schema with TagSupport] = sourceGraphs.map(g => g -> context.schemaCatalog(g)).toMap
              val retaggings: Map[QualifiedGraphName, Map[Int, Int]] = computeRetaggings(schemasForGraphs)
              val constructGraphOperator = producer.planConstructGraph(process(in), construct, retaggings)
              if (onGraph.isEmpty) { // No on-graphs, the retagged constructed graph is enough
                constructGraphOperator
              } else { // Multiple graphs, might need to do retagging
                val constructOnGraph: P = {
                  val retaggingsForGraphs = construct.onGraphs.map(qgn => catalogGraph(qgn) -> retaggings(qgn)).toMap
                  // Create UNION graph for `onGraphs`
                  val onGraphUnion = producer.planGraphUnionAll(retaggingsForGraphs.keys.toList, retaggingsForGraphs)
                  onGraphUnion
                }
                // Another union between the on-graphs with the constructed pattern graph, no retaggings
                producer.planGraphUnionAll(List(constructOnGraph, constructGraphOperator))
              }
            }
        }

      case op
        @flat.NodeScan(v, in, header)
      =>
        producer.planNodeScan(process(in), op.sourceGraph, v, header)

      case op
        @flat.EdgeScan(e, in, header)
      =>
        producer.planRelationshipScan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header)
      =>
        producer.planAlias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header)
      =>
        producer.planUnwind(process(in), list, item, header)

      case flat.Project(expr, in, header)
      =>
        producer.planProject(process(in), expr, header)

      case flat.Aggregate(aggregations, group, in, header)
      =>
        producer.planAggregate(process(in), group, aggregations, header)

      case flat.Filter(expr, in, header)
      =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            producer.planFilter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions, header)

      case flat.Distinct(fields, in, _) =>
        producer.planDistinct(process(in), fields)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val startFrom = sourceOp.sourceGraph match {
          case e: LogicalCatalogGraph =>
            producer.planStart(None, Some(e.qualifiedGraphName))

          case c: LogicalPatternGraph =>
            producer.planConstructGraph(producer.planStart(Some(context.inputRecords)), c, Map.empty)
        }

        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel, relHeader)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            producer.planJoin(tempResult, third, Seq(endNode -> target), header)

          case Undirected =>
            val tempOutgoing = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            val outgoing = producer.planJoin(tempOutgoing, third, Seq(endNode -> target), header)

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = producer.planFilter(second, filterExpression, second.header)

            val tempIncoming = producer.planJoin(third, relsWithoutLoops, Seq(target -> startNode), third.header ++ second.header)
            val incoming = producer.planJoin(tempIncoming, first, Seq(endNode -> source), header)

            producer.planTabularUnionAll(outgoing, incoming)
        }

      case op
        @flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader)
      =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel, relHeader)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode), header)
            producer.planTabularUnionAll(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header)
      =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
      rel, edgeList, target, direction,
      lower, upper,
      sourceOp, relOp, targetOp,
      header, isExpandInto)
      =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        producer.planBoundedVarExpand(
          first,
          second,
          third,
          rel,
          edgeList,
          target,
          sourceOp.endNode,
          lower,
          upper,
          direction,
          header,
          isExpandInto)

      case flat.Optional(lhs, rhs, header)
      =>
        producer.planOptional(process(lhs), process(rhs), header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header)
      =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header)
      =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header)
      =>
        producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header)
      =>
        producer.planLimit(process(in), expr, header)

      case flat.ReturnGraph(in)
      =>
        producer.planReturnGraph(process(in))

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String]

  = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
