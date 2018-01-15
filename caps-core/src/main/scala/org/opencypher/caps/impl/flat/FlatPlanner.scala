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
package org.opencypher.caps.impl.flat

import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.logical.LogicalOperator
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.record.{ProjectedExpr, ProjectedField}
import org.opencypher.caps.impl.{DirectCompilationStage, logical}

final case class FlatPlannerContext(parameters: Map[String, CypherValue])

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.CartesianProduct(lhs, rhs, _) =>
        producer.cartesianProduct(process(lhs), process(rhs))

      case logical.Select(fields, graphs, in, _) =>
        val withAliasesRemoved = producer.removeAliases(fields, process(in))
        producer.select(fields, graphs, withAliasesRemoved)

      case logical.Filter(expr, in, _) =>
        producer.filter(expr, process(in))

      case logical.Distinct(fields, in, _) =>
        producer.distinct(fields, process(in))

      case logical.NodeScan(node, in, _) =>
        producer.nodeScan(node, process(in))

      case logical.Unwind(list, item, in, _) =>
        producer.unwind(list, item, process(in))

      case logical.Project(expr, None, in, _) =>
        producer.project(ProjectedExpr(expr), process(in))

      case logical.Project(expr, Some(field), in, _) =>
        producer.project(ProjectedField(field, expr), process(in))

      case logical.ProjectGraph(graph, in, _) =>
        val prev = process(in)
        ProjectGraph(graph, prev, prev.header)

      case logical.Aggregate(aggregations, group, in, _) =>
        producer.aggregate(aggregations, group, process(in))

      case logical.ExpandSource(source, rel, target, sourceOp, targetOp, _) =>
        producer.expandSource(source, rel, target, input.sourceGraph.schema, process(sourceOp), process(targetOp))

      case logical.ExpandInto(source, rel, target, sourceOp, _) =>
        producer.expandInto(source, rel, target, input.sourceGraph.schema, process(sourceOp))

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        producer.valueJoin(process(lhs), process(rhs), predicates)

      case logical.EmptyRecords(fields, in, _) =>
        producer.planEmptyRecords(fields, process(in))

      case logical.Start(graph, fields, _) =>
        producer.planStart(graph, fields)

      case logical.SetSourceGraph(graph, in, _) =>
        producer.planSetSourceGraph(graph, process(in))

      case logical.BoundedVarLengthExpand(source, edgeList, target, lower, upper, sourceOp, targetOp, _) =>
        val initVarExpand = producer.initVarExpand(source, edgeList, process(sourceOp))
        val edgeScan = producer.varLengthEdgeScan(edgeList, producer.planStart(input.sourceGraph, Set.empty))
        producer.boundedVarExpand(
          edgeScan.edge,
          edgeList,
          target,
          lower,
          upper,
          initVarExpand,
          edgeScan,
          process(targetOp),
          isExpandInto = sourceOp == targetOp)

      case logical.Optional(lhs, rhs, _) =>
        producer.planOptional(process(lhs), process(rhs))

      case logical.ExistsPatternPredicate(expr, lhs, rhs, _) =>
        producer.planExistsPatternPredicate(expr, process(lhs), process(rhs))

      case logical.OrderBy(sortListItems, sourceOp, _) =>
        producer.orderBy(sortListItems, process(sourceOp))

      case logical.Skip(expr, sourceOp, _) =>
        producer.skip(expr, process(sourceOp))

      case logical.Limit(expr, sourceOp, _) =>
        producer.limit(expr, process(sourceOp))

      case x =>
        Raise.notYetImplemented(s"Flat planning not done yet for $x")
    }
  }
}
