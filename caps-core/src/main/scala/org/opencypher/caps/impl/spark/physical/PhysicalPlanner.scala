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
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.logical.{LogicalExternalGraph, LogicalPatternGraph}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.{DirectCompilationStage, flat}
import org.opencypher.caps.ir.api.block.SortItem

case class PhysicalPlannerContext(
   resolver: URI => CAPSGraph,
   inputRecords: CAPSRecords,
   parameters: Map[String, CypherValue])

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalResult, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalResult = {

    val producer = new PhysicalResultProducer(RuntimeContext(context.parameters))
    import producer._

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        process(lhs).cartesianProduct(process(rhs), header)

      case flat.Select(fields, graphs, in, header) =>
        process(in).select(fields, header).selectGraphs(graphs)

      case flat.Start(graph, _) => graph match {
        case LogicalExternalGraph(name, uri, _) =>
          val graph = context.resolver(uri)
          PhysicalResult(context.inputRecords, Map(name -> graph))

        case _ =>
          Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case flat.SetSourceGraph(graph, in, header) =>
        val p = process(in)

        graph match {
          case LogicalExternalGraph(name, uri, _) =>
            val graph = context.resolver(uri)
            p.withGraph(name -> graph)

          case _ =>
            Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        process(in).nodeScan(op.sourceGraph, v, labels, header)

      case op@flat.EdgeScan(e, edgeDef, in, header) =>
        process(in).relationshipScan(op.sourceGraph, e, edgeDef, header)

      case flat.Alias(expr, alias, in, header) =>
        process(in).alias(expr, alias, header)

      case flat.Project(expr, in, header) =>
        process(in).project(expr, header)

      case flat.ProjectGraph(graph, in, _) => graph match {
        case LogicalExternalGraph(name, uri, _) =>
          val capsGraph = context.resolver(uri)
          process(in).withGraph(name -> capsGraph)
        case graph: LogicalPatternGraph =>
          process(in).projectGraph(graph)
      }

      case flat.Aggregate(aggregations, group, in , header) =>
        process(in).aggregate(aggregations, group, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() => process(in) // optimise away filter
        case _ => process(in).filter(expr, header)
      }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        process(lhs).valueJoin(process(rhs), predicates, header)

      case flat.Distinct(in, header) =>
        process(in).distinct(header)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of using the graph
      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, sourceOp, targetOp, header, relHeader) =>
        val lhs = process(sourceOp)
        val rhs = process(targetOp)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val g = lhs.graphs(op.sourceGraph.name)
        val relationships = g.relationships(rel.name, ctRelType)
        val relRhs = PhysicalResult(relationships, lhs.graphs)
          .typeFilter(rel, types.relTypes, relHeader)

        val relAndTargetHeader = relRhs.records.header ++ rhs.records.header
        val relAndTarget = relRhs.joinTarget(rhs, relAndTargetHeader).on(rel)(target)
        val expanded = lhs.joinSource(relAndTarget, header).on(source)(rel)

        expanded

      case op@flat.ExpandInto(source, rel, types, target, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val g = in.graphs(op.sourceGraph.name)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val relationships = PhysicalResult(g.relationships(rel.name, ctRelType), in.graphs)
          .typeFilter(rel, types.relTypes, relHeader)
        // in join rels on source == rel.source and target == rels.target
        in.joinInto(relationships, header).on(source, target)(rel)

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        val prev = process(in)
        prev.initVarExpand(source, edgeList, endNode, header)

      case flat.BoundedVarExpand(rel, edgeList, target, lower, upper, sourceOp, relOp, targetOp, header, isExpandInto) =>
        val first  = process(sourceOp)
        val second = process(relOp)
        val third  = process(targetOp)

        val expanded = first.varExpand(second, edgeList, sourceOp.endNode, rel, lower, upper, header)
        expanded.finalizeVarExpand(third, sourceOp.endNode, target, header, isExpandInto)

      case flat.Optional(lhs, rhs, lhsHeader, rhsHeader) =>
        process(lhs).optional(process(rhs), lhsHeader, rhsHeader)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        process(in).orderBy(sortItems, header)

      case flat.Skip(expr, in, header) =>
        process(in).skip(expr, header)

      case flat.Limit(expr, in, header) =>
        process(in).limit(expr, header)

      case x =>
        Raise.notYetImplemented(s"physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
