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
package org.opencypher.okapi.relational.impl.flat

import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.logical.{impl => logical}
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField}

final case class FlatPlannerContext(parameters: CypherMap)

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.CartesianProduct(lhs, rhs, _) =>
        producer.cartesianProduct(process(lhs), process(rhs))

      case logical.Select(fields, in, _) =>
        val withAliasesRemoved = if (fields.nonEmpty) {
          producer.removeAliases(fields, process(in))
        } else process(in)
        producer.select(fields, withAliasesRemoved)

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

      case logical.Aggregate(aggregations, group, in, _) =>
        producer.aggregate(aggregations, group, process(in))

      case logical.Expand(source, rel, direction, target, sourceOp, targetOp, _) =>
        producer.expand(source, rel, target, direction, input.graph.schema, process(sourceOp), process(targetOp))

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        producer.expandInto(source, rel, target, direction, input.graph.schema, process(sourceOp))

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        producer.valueJoin(process(lhs), process(rhs), predicates)

      case logical.EmptyRecords(fields, in, _) =>
        producer.planEmptyRecords(fields, process(in))

      case logical.Start(graph, fields, _) =>
        producer.planStart(graph, fields)

      case logical.UseGraph(graph, in, _) =>
        producer.planUseGraph(graph, process(in))

      case logical.BoundedVarLengthExpand(source, edgeList, target, direction, lower, upper, sourceOp, targetOp, _) =>
        val initVarExpand = producer.initVarExpand(source, edgeList, process(sourceOp))
        val edgeScan = producer.varLengthEdgeScan(edgeList, producer.planStart(input.graph, Set.empty))
        producer.boundedVarExpand(
          edgeScan.edge,
          edgeList,
          target,
          direction,
          lower,
          upper,
          initVarExpand,
          edgeScan,
          process(targetOp),
          isExpandInto = sourceOp == targetOp)

      case logical.Optional(lhs, rhs, _) =>
        producer.planOptional(process(lhs), process(rhs))

      case logical.ExistsSubQuery(expr, lhs, rhs, _) =>
        producer.planExistsSubQuery(expr, process(lhs), process(rhs))

      case logical.OrderBy(sortListItems, sourceOp, _) =>
        producer.orderBy(sortListItems, process(sourceOp))

      case logical.Skip(expr, sourceOp, _) =>
        producer.skip(expr, process(sourceOp))

      case logical.Limit(expr, sourceOp, _) =>
        producer.limit(expr, process(sourceOp))

      case logical.ReturnGraph(in, _) =>
        producer.returnGraph(process(in))

      case x =>
        throw NotImplementedException(s"Flat planning not implemented for $x")
    }
  }
}
