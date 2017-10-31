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
package org.opencypher.caps.impl.flat

import org.opencypher.caps.api.types.{CTList, CTRelationship, CypherType}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.logical.LogicalOperator
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.{DirectCompilationStage, logical}
import org.opencypher.caps.ir.api.RelType
import org.opencypher.caps.ir.api.pattern.{AnyGiven, EveryRelationship}

import scala.annotation.tailrec

final case class FlatPlannerContext(parameters: Map[String, CypherValue])

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.CartesianProduct(lhs, rhs) =>
        producer.cartesianProduct(process(lhs), process(rhs))

      case logical.Select(fields, graphs, in) =>
        producer.select(fields, graphs, process(in))

      case logical.Filter(expr, in) =>
        producer.filter(expr, process(in))

      case logical.Distinct(fields, in) =>
        producer.distinct(fields, process(in))

      case logical.NodeScan(node, nodeDef, in) =>
        producer.nodeScan(node, nodeDef, process(in))

      case logical.Project(it, in) =>
        producer.project(it, process(in))

      case logical.ProjectGraph(graph, in) =>
        val prev = process(in)
        ProjectGraph(graph, prev, prev.header)

      case logical.Aggregate(aggregations, group, in) =>
        producer.aggregate(aggregations, group, process(in))

      case logical.ExpandSource(source, rel, types, target, sourceOp, targetOp) =>
        producer.expandSource(source, rel, types, target, input.sourceGraph.schema, process(sourceOp), process(targetOp))

      case logical.ExpandInto(source, rel, types, target, sourceOp) =>
        producer.expandInto(source, rel, types, target, input.sourceGraph.schema, process(sourceOp))

      case logical.ValueJoin(lhs, rhs, predicates) =>
        producer.valueJoin(process(lhs), process(rhs), predicates)

      case logical.Start(graph, fields) =>
        producer.planStart(graph, fields)

      case logical.SetSourceGraph(graph, in) =>
        producer.planSetSourceGraph(graph, process(in))

      case logical.BoundedVarLengthExpand(source, edgeList, target, lower, upper, sourceOp, targetOp) =>
        val initVarExpand = producer.initVarExpand(source, edgeList, process(sourceOp))
        val types: Set[RelType] = relTypeFromList(edgeList.cypherType).map(RelType)
        val edgeScan = producer.varLengthEdgeScan(edgeList, EveryRelationship(AnyGiven(types)), producer.planStart(input.sourceGraph, Set.empty))
        producer.boundedVarExpand(edgeScan.edge, edgeList, target, lower, upper, initVarExpand,
          edgeScan, process(targetOp), isExpandInto = sourceOp == targetOp)

      case logical.Optional(lhs, rhs) =>
        producer.planOptional(process(lhs), process(rhs))

      case logical.OrderBy(sortListItems, sourceOp) =>
        producer.orderBy(sortListItems, process(sourceOp))

      case logical.Skip(expr, sourceOp) =>
        producer.skip(expr, process(sourceOp))

      case logical.Limit(expr, sourceOp) =>
        producer.limit(expr, process(sourceOp))

      case x =>
        Raise.notYetImplemented(s"Flat planning not done yet for $x")
    }
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = t match {
    case l: CTList => relTypeFromList(l.elementType)
    case r: CTRelationship => r.types
    case _ => Raise.impossible()
  }
}
