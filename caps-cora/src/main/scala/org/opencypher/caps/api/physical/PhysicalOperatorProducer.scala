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
package org.opencypher.caps.api.physical

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.record.{CypherRecords, ProjectedExpr, ProjectedField, RecordHeader}
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.caps.logical.impl.{ConstructedEntity, Direction, LogicalExternalGraph, LogicalGraph}

trait PhysicalOperatorProducer[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]] {

  def planCartesianProduct(lhs: P, rhs: P, header: RecordHeader): P

  def planRemoveAliases(in: P, dependent: Set[(ProjectedField, ProjectedExpr)], header: RecordHeader): P

  def planSelectFields(in: P, fields: IndexedSeq[Var], header: RecordHeader): P

  def planSelectGraphs(in: P, graphs: Set[String]): P

  def planEmptyRecords(in: P, header: RecordHeader): P

  def planStart(in: R, g: LogicalExternalGraph): P

  def planSetSourceGraph(in: P, g: LogicalExternalGraph): P

  def planNodeScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  def planRelationshipScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  def planAlias(in: P, expr: Expr, alias: Var, header: RecordHeader): P

  def planUnwind(in: P, list: Expr, item: Var, header: RecordHeader): P

  def planProject(in: P, expr: Expr, header: RecordHeader): P

  def planProjectExternalGraph(in: P, name: String, uri: URI): P

  def planProjectPatternGraph(
    in: P,
    toCreate: Set[ConstructedEntity],
    name: String,
    schema: Schema,
    header: RecordHeader): P

  def planAggregate(in: P, aggregations: Set[(Var, Aggregator)], group: Set[Var], header: RecordHeader): P

  def planFilter(in: P, expr: Expr, header: RecordHeader): P

  def planValueJoin(lhs: P, rhs: P, predicates: Set[org.opencypher.caps.ir.api.expr.Equals], header: RecordHeader): P

  def planDistinct(in: P, fields: Set[Var]): P

  def planStartFromUnit(graph: LogicalExternalGraph): P

  def planExpandSource(
    first: P,
    second: P,
    third: P,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader,
    removeSelfRelationships: Boolean = false): P

  def planUnion(lhs: P, rhs: P): P

  def planExpandInto(lhs: P, rhs: P, source: Var, rel: Var, target: Var, header: RecordHeader): P

  def planInitVarExpand(in: P, source: Var, edgeList: Var, target: Var, header: RecordHeader): P

  def planBoundedVarExpand(
    first: P,
    second: P,
    third: P,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean): P

  def planOptional(lhs: P, rhs: P, header: RecordHeader): P

  def planExistsSubQuery(lhs: P, rhs: P, targetField: Var, header: RecordHeader): P

  def planOrderBy(in: P, sortItems: Seq[SortItem[Expr]], header: RecordHeader): P

  def planSkip(in: P, expr: Expr, header: RecordHeader): P

  def planLimit(in: P, expr: Expr, header: RecordHeader): P
}
