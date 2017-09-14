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
package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.logical.{AmbientLogicalGraph, ExternalLogicalGraph}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.{DirectCompilationStage, flat}
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.global._

case class PhysicalPlannerContext(
   ambientGraph: CAPSGraph,
   inputRecords: CAPSRecords,
   tokens: TokenRegistry,
   constants: ConstantRegistry,
   parameters: Map[ConstantRef, CypherValue])

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalResult, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalResult = {
    inner(flatPlan)
  }

  def inner(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalResult = {

    import context.tokens

    val producer = new PhysicalResultProducer(RuntimeContext(context.parameters, context.tokens, context.constants))
    import producer._

    flatPlan match {
      case flat.Select(fields, in, header) =>
        inner(in).select(fields, header)

      case flat.Start(graph, _) => graph match {
        case ExternalLogicalGraph(name, uri, _) =>
          val graph = context.ambientGraph.session.graphAt(uri)
          PhysicalResult(context.inputRecords, Map(name -> graph))

        case a: AmbientLogicalGraph =>
          PhysicalResult(context.inputRecords, Map(a.name -> context.ambientGraph))
        case _ =>
          Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case flat.SetSourceGraph(graph, in, header) =>
        val p = inner(in)

        graph match {
          case ExternalLogicalGraph(name, uri, _) =>
            val graph = context.ambientGraph.session.graphAt(uri)
            p.withGraph(name -> graph)
          case a: AmbientLogicalGraph =>
            p.withGraph(a.name -> context.ambientGraph)
          case _ =>
            Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        inner(in).nodeScan(op.sourceGraph, v, labels, header)

      case op@flat.EdgeScan(e, edgeDef, in, header) =>
        inner(in).relationshipScan(op.sourceGraph, e, edgeDef, header)

      case flat.Alias(expr, alias, in, header) =>
        inner(in).alias(expr, alias, header)

      case flat.Project(expr, in, header) =>
        inner(in).project(expr, header)

      case flat.Aggregate(aggregations, group, in , header) =>
        inner(in).aggregate(aggregations, group, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() => inner(in) // optimise away filter
        case _ => inner(in).filter(expr, header)
      }

      case flat.Distinct(in, header) =>
        inner(in).distinct(header)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of using the graph
      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, sourceOp, targetOp, header, relHeader) =>
        val lhs = inner(sourceOp)
        val rhs = inner(targetOp)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val g = lhs.graphs(op.sourceGraph.name)
        val relationships = g.relationships(rel.name, ctRelType)
        val relRhs = PhysicalResult(relationships, lhs.graphs).typeFilter(rel, types.relTypes.map(tokens.relTypeRef), relHeader)

        val relAndTargetHeader = relRhs.records.details.header ++ rhs.records.details.header
        val relAndTarget = relRhs.joinTarget(rhs, relAndTargetHeader).on(rel)(target)
        val expanded = lhs.joinSource(relAndTarget, header).on(source)(rel)

        expanded

      case op@flat.ExpandInto(source, rel, types, target, sourceOp, header, relHeader) =>
        val in = inner(sourceOp)
        val g = in.graphs(op.sourceGraph.name)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val relationships = PhysicalResult(g.relationships(rel.name, ctRelType), in.graphs)
          .typeFilter(rel, types.relTypes.map(tokens.relTypeRef), relHeader)
        // in join rels on source == rel.source and target == rels.target
        in.joinInto(relationships, header).on(source, target)(rel)

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        val prev = inner(in)
        prev.initVarExpand(source, edgeList, endNode, header)

      case flat.BoundedVarExpand(rel, edgeList, target, lower, upper, sourceOp, relOp, targetOp, header) =>
        val first  = inner(sourceOp)
        val second = inner(relOp)
        val third  = inner(targetOp)

        val expanded = first.varExpand(second, edgeList, sourceOp.endNode, rel, lower, upper, header)
        expanded.finalizeVarExpand(third, sourceOp.endNode, target, header)

      case flat.Optional(lhs, rhs, lhsHeader, rhsHeader) =>
        inner(lhs).optional(inner(rhs), lhsHeader, rhsHeader)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        inner(in).orderBy(sortItems, header)

      case flat.Skip(expr, in, header) =>
        inner(in).skip(expr, header)

      case flat.Limit(expr, in, header) =>
        inner(in).limit(expr, header)

      case x =>
        Raise.notYetImplemented(s"operator $x")
    }
  }

  private def relTypes(r: Var, tokens: TokenRegistry): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
