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
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
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

            val constructGraphOperator = producer.planConstructGraph(process(in), construct, context.catalog)

            // Create UNION graph for `onGraphs`
            val constructOnGraph: Option[P] = construct.onGraphs match {
              case Nil =>
                None
              case h :: Nil =>
                Some(catalogGraph(h))
              case severalGraphs =>
                val onGraphUnion = producer.planGraphUnionAll(severalGraphs.map(catalogGraph))
                Some(onGraphUnion)
            }

            // If necessary plan a union if there are graphs that the new graph is CONSTRUCTED ON.
            constructOnGraph match {
              case None => constructGraphOperator
              // Do not prevent collisions with the constructed graph in order to preserve the constructed relationships
              case Some(constructedOn) => producer.planGraphUnionAll(constructedOn :: constructGraphOperator :: Nil, preventIdCollisions = false)
            }
        }

      case op@flat.NodeScan(v, in, header) =>
        producer.planNodeScan(process(in), op.sourceGraph, v, header)

      case op@flat.EdgeScan(e, in, header) =>
        producer.planRelationshipScan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) =>
        producer.planAlias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header) =>
        producer.planUnwind(process(in), list, item, header)

      case flat.Project(expr, in, header) =>
        producer.planProject(process(in), expr, header)

      case flat.Aggregate(aggregations, group, in, header) =>
        producer.planAggregate(process(in), group, aggregations, header)

      case flat.Filter(expr, in, header) =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            producer.planFilter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        producer.planValueJoin(process(lhs), process(rhs), predicates, header)

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
            producer.planConstructGraph(producer.planStart(Some(context.inputRecords)), c, context.catalog)
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

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
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

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
      rel, edgeList, target, direction,
      lower, upper,
      sourceOp, relOp, targetOp,
      header, isExpandInto) =>
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

      case flat.Optional(lhs, rhs, header) =>
        producer.planOptional(process(lhs), process(rhs), header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) =>
        producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header) =>
        producer.planLimit(process(in), expr, header)

      case flat.ReturnGraph(in) =>
        producer.planReturnGraph(process(in))

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
