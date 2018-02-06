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
 */
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.flat
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.spark.CAPSRecords
import org.opencypher.caps.impl.spark.physical.operators._
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.logical.impl._

case class PhysicalPlannerContext(
  resolver: URI => PropertyGraph,
  inputRecords: CAPSRecords,
  parameters: CypherMap)

object PhysicalPlannerContext {
  def from(
    resolver: URI => PropertyGraph,
    inputRecords: CAPSRecords,
    parameters: CypherMap): PhysicalPlannerContext = {
    new PhysicalPlannerContext(resolver, inputRecords, parameters)
  }

}

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalOperator, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalOperator = {

    implicit val caps = context.inputRecords.caps

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        operators.CartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        RemoveAliases(dependent, process(in), header)

      case flat.Select(fields, graphs, in, header) =>
        val selected = SelectFields(process(in), fields, header)
        SelectGraphs(selected, graphs)

      case flat.EmptyRecords(in, header) =>
        operators.EmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            operators.Start(context.inputRecords, g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case flat.SetSourceGraph(graph, in, header) =>
        graph match {
          case g: LogicalExternalGraph =>
            operators.SetSourceGraph(process(in), g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case op@flat.NodeScan(v, in, header) =>
        Scan(process(in), op.sourceGraph, v, header)

      case op@flat.EdgeScan(e, in, header) =>
        Scan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) =>
        Alias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header) =>
        operators.Unwind(process(in), list, item, header)

      case flat.Project(expr, in, header) =>
        operators.Project(process(in), expr, header)

      case flat.ProjectGraph(graph, in, header) =>
        graph match {
          case LogicalExternalGraph(name, uri, _) =>
            ProjectExternalGraph(process(in), name, uri)
          case LogicalPatternGraph(name, targetSchema, GraphOfPattern(toCreate, _)) =>
            ProjectPatternGraph(process(in), toCreate, name, targetSchema, header)
        }

      case flat.Aggregate(aggregations, group, in, header) =>
        operators.Aggregate(process(in), aggregations, group, header)

      case flat.Filter(expr, in, header) =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            operators.Filter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        operators.ValueJoin(process(lhs), process(rhs), predicates, header)

      case flat.Distinct(fields, in, _) =>
        operators.Distinct(fields, process(in))

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val externalGraph = sourceOp.sourceGraph match {
          case e: LogicalExternalGraph => e
          case _ => throw IllegalArgumentException("a LogicalExternalGraph", sourceOp.sourceGraph)
        }

        val startFrom = StartFromUnit(externalGraph)
        val second = Scan(startFrom, op.sourceGraph, rel, relHeader)

        direction match {
          case Directed =>
            ExpandSource(first, second, third, source, rel, target, header)
          case Undirected =>
            val outgoing = ExpandSource(first, second, third, source, rel, target, header)
            val incoming = ExpandSource(third, second, first, target, rel, source, header, removeSelfRelationships = true)
            Union(outgoing, incoming)
        }


      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val relationships = Scan(in, op.sourceGraph, rel, relHeader)

        direction match {
          case Directed =>
            operators.ExpandInto(in, relationships, source, rel, target, header)
          case Undirected =>
            val outgoing = operators.ExpandInto(in, relationships, source, rel, target, header)
            val incoming = operators.ExpandInto(in, relationships, target, rel, source, header)
            Union(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        InitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
          rel, edgeList, target, direction,
          lower, upper,
          sourceOp, relOp, targetOp,
          header, isExpandInto) =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        BoundedVarExpand(
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
        operators.Optional(process(lhs), process(rhs), header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        operators.ExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        operators.OrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) =>
        operators.Skip(process(in), expr, header)

      case flat.Limit(expr, in, header) =>
        operators.Limit(process(in), expr, header)

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
