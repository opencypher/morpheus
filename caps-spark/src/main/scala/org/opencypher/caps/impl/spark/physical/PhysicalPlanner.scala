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
import org.opencypher.caps.api.graph.CypherGraph
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.flat
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.spark.physical.operators._
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.logical.impl.{GraphOfPattern, LogicalExternalGraph, LogicalPatternGraph}

case class PhysicalPlannerContext(
    resolver: URI => CypherGraph,
    inputRecords: CAPSRecords,
    parameters: Map[String, CypherValue])

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalOperator, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalOperator = {

    implicit val caps = context.inputRecords.caps

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        CartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        RemoveAliases(dependent, process(in), header)

      case flat.Select(fields, graphs, in, header) =>
        val selected = SelectFields(process(in), fields, Some(header))
        SelectGraphs(selected, graphs)

      case flat.EmptyRecords(in, header) =>
        EmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            Start(context.inputRecords, g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case flat.SetSourceGraph(graph, in, header) =>
        graph match {
          case g: LogicalExternalGraph =>
            SetSourceGraph(process(in), g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case op @ flat.NodeScan(v, in, header) =>
        Scan(process(in), op.sourceGraph, v, header)

      case op @ flat.EdgeScan(e, in, header) =>
        Scan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) =>
        Alias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header) =>
        Unwind(process(in), list, item, header)

      case flat.Project(expr, in, header) =>
        Project(process(in), expr, header)

      case flat.ProjectGraph(graph, in, _) =>
        graph match {
          case LogicalExternalGraph(name, uri, _) =>
            ProjectExternalGraph(process(in), name, uri)
          case LogicalPatternGraph(name, targetSchema, GraphOfPattern(toCreate, toRetain)) =>
            val select = SelectFields(process(in), toRetain.toIndexedSeq, None)
            val distinct = SimpleDistinct(select)
            ProjectPatternGraph(distinct, toCreate, name, targetSchema)
        }

      case flat.Aggregate(aggregations, group, in, header) =>
        Aggregate(process(in), aggregations, group, header)

      case flat.Filter(expr, in, header) =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            Filter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        ValueJoin(process(lhs), process(rhs), predicates, header)

      case flat.Distinct(in, header) =>
        Distinct(process(in), header)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op @ flat.ExpandSource(source, rel, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val externalGraph = sourceOp.sourceGraph match {
          case e: LogicalExternalGraph => e
          case _                       => throw IllegalArgumentException("a LogicalExternalGraph", sourceOp.sourceGraph)
        }

        val startFrom = StartFromUnit(externalGraph)
        val second = Scan(startFrom, op.sourceGraph, rel, relHeader)

        ExpandSource(first, second, third, source, rel, target, header)

      case op @ flat.ExpandInto(source, rel, target, sourceOp, header, relHeader) =>
        val in = process(sourceOp)

        val rels = Scan(in, op.sourceGraph, rel, relHeader)

        ExpandInto(in, rels, source, rel, target, header)

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        InitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
          rel,
          edgeList,
          target,
          lower,
          upper,
          sourceOp,
          relOp,
          targetOp,
          header,
          isExpandInto) =>
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
          header,
          isExpandInto)

      case flat.Optional(lhs, rhs, header) =>
        Optional(process(lhs), process(rhs), header)

      case flat.ExistsPatternPredicate(predicateField, lhs, rhs, header) =>
        ExistsPatternPredicate(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        OrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) =>
        Skip(process(in), expr, header)

      case flat.Limit(expr, in, header) =>
        Limit(process(in), expr, header)

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _                 => Set.empty
  }
}
