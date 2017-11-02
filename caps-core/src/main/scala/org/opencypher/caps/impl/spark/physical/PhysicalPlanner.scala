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
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.logical.{GraphOfPattern, LogicalExternalGraph, LogicalPatternGraph}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.{DirectCompilationStage, flat}
import org.opencypher.caps.ir.api.block.SortItem

case class PhysicalPlannerContext(
   resolver: URI => CAPSGraph,
   inputRecords: CAPSRecords,
   parameters: Map[String, CypherValue])

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalOperator, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalOperator = {

    implicit val caps = context.inputRecords.caps

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        CartesianProduct(header, process(lhs), process(rhs))

      case flat.Select(fields, graphs, in, header) =>
        val selected = SelectFields(fields, Some(header), process(in))
        SelectGraphs(graphs, selected)

      case flat.Start(graph, _) => graph match {
        case g: LogicalExternalGraph =>
          Start(g, context.inputRecords)

        case _ =>
          Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case flat.SetSourceGraph(graph, in, header) =>
        graph match {
          case g: LogicalExternalGraph =>
            SetSourceGraph(g, process(in))

          case _ =>
            Raise.impossible(s"Got an unknown type of graph to start from: $graph")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        // TODO: propagate to earlier stage
        val vType = CTNode(labels.labels.elements.map(_.name).toSeq: _*)

        Scan(op.sourceGraph, Var(v.name)(vType), process(in))

      case op@flat.EdgeScan(e, edgeDef, in, header) =>
        // TODO: propagate to earlier stage
        val vType = CTRelationship(edgeDef.relTypes.elements.map(_.name).toSeq: _*)

        Scan(op.sourceGraph, Var(e.name)(vType), process(in))

      case flat.Alias(expr, alias, in, header) =>
        Alias(expr, alias, header, process(in))

      case flat.Project(expr, in, header) =>
        Project(expr, header, process(in))

      case flat.ProjectGraph(graph, in, _) => graph match {
        case LogicalExternalGraph(name, uri, _) =>
          ProjectExternalGraph(name, uri, process(in))
        case LogicalPatternGraph(name, targetSchema, GraphOfPattern(toCreate, toRetain)) =>
          val select = SelectFields(toRetain.toIndexedSeq, None, process(in))
          val distinct = SimpleDistinct(select)
          ProjectPatternGraph(toCreate, name, targetSchema, distinct)
      }

      case flat.Aggregate(aggregations, group, in, header) =>
        Aggregate(aggregations, group, header, process(in))

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() =>
          process(in) // optimise away filter
        case _ =>
          Filter(expr, header, process(in))
      }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        ValueJoin(predicates, header, process(lhs), process(rhs))

      case flat.Distinct(in, header) =>
        Distinct(header, process(in))

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of using the graph
      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val relWithType = Var(rel.name)(ctRelType)
        val externalGraph = sourceOp.sourceGraph match {
          case e: LogicalExternalGraph => e
          case _ => Raise.invalidArgument("an external graph", sourceOp.sourceGraph.toString)
        }
        val second = Scan(op.sourceGraph, relWithType, StartFrom(CAPSRecords.unit(), externalGraph))

        ExpandSource(source, relWithType, target, header, first, second, third)

      case op@flat.ExpandInto(source, rel, types, target, sourceOp, header, relHeader) =>
        val in = process(sourceOp)

        val ctRelType = CTRelationship.apply(types.relTypes.elements.map(_.name))
        val relWithType = Var(rel.name)(ctRelType)
        val rels = Scan(op.sourceGraph, relWithType, in)

        ExpandInto(source, relWithType, target, header, in, rels)

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        InitVarExpand(source, edgeList, endNode, header, process(in))

      case flat.BoundedVarExpand(rel, edgeList, target, lower, upper, sourceOp, relOp, targetOp, header, isExpandInto) =>
        val first  = process(sourceOp)
        val second = process(relOp)
        val third  = process(targetOp)

        BoundedVarExpand(rel, edgeList, target, sourceOp.endNode, lower, upper, header, first, second, third, isExpandInto)

      case flat.Optional(lhs, rhs, lhsHeader, rhsHeader) =>
        Optional(lhsHeader, rhsHeader, process(lhs), process(rhs))

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        OrderBy(sortItems, header, process(in))

      case flat.Skip(expr, in, header) =>
        Skip(expr, header, process(in))

      case flat.Limit(expr, in, header) =>
        Limit(expr, header, process(in))

      case x =>
        Raise.notYetImplemented(s"physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
