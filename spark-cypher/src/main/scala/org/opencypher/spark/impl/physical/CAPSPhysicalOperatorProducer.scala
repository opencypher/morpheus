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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField, RecordHeader}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}

case class CAPSPhysicalPlannerContext(
  session: CAPSSession,
  resolver: QualifiedGraphName => PropertyGraph,
  inputRecords: CAPSRecords,
  parameters: CypherMap) extends PhysicalPlannerContext[CAPSRecords]

object CAPSPhysicalPlannerContext {
  def from(
    resolver: QualifiedGraphName => PropertyGraph,
    inputRecords: CAPSRecords,
    parameters: CypherMap)(implicit session: CAPSSession): PhysicalPlannerContext[CAPSRecords] = {
    CAPSPhysicalPlannerContext(session, resolver, inputRecords, parameters)
  }
}

final class CAPSPhysicalOperatorProducer(implicit caps: CAPSSession)
  extends PhysicalOperatorProducer[CAPSPhysicalOperator, CAPSRecords, CAPSGraph, CAPSRuntimeContext] {

  override def planCartesianProduct(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    header: RecordHeader): CAPSPhysicalOperator = operators.CartesianProduct(lhs, rhs, header)

  override def planRemoveAliases(
    in: CAPSPhysicalOperator,
    dependent: Set[(ProjectedField, ProjectedExpr)],
    header: RecordHeader): CAPSPhysicalOperator = operators.RemoveAliases(in, dependent, header)

  override def planSelectFields(in: CAPSPhysicalOperator, fields: IndexedSeq[Var], header: RecordHeader): CAPSPhysicalOperator =
    operators.SelectFields(in, fields, header)

  override def planSelectGraphs(in: CAPSPhysicalOperator, graphs: Set[String]): CAPSPhysicalOperator =
    operators.SelectGraphs(in, graphs)

  override def planEmptyRecords(in: CAPSPhysicalOperator, header: RecordHeader): CAPSPhysicalOperator =
    operators.EmptyRecords(in, header)

  override def planStart(in: CAPSRecords, g: LogicalExternalGraph): CAPSPhysicalOperator =
    operators.Start(in, g)

  override def planSetSourceGraph(in: CAPSPhysicalOperator, g: LogicalExternalGraph): CAPSPhysicalOperator =
    operators.SetSourceGraph(in, g)

  override def planNodeScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.Scan(in, inGraph, v, header)

  override def planRelationshipScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.Scan(in, inGraph, v, header)

  override def planAlias(in: CAPSPhysicalOperator, expr: Expr, alias: Var, header: RecordHeader): CAPSPhysicalOperator =
    operators.Alias(in, expr, alias, header)

  override def planUnwind(in: CAPSPhysicalOperator, list: Expr, item: Var, header: RecordHeader): CAPSPhysicalOperator =
    operators.Unwind(in, list, item, header)

  override def planProject(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.Project(in, expr, header)

  override def planProjectExternalGraph(in: CAPSPhysicalOperator, name: String, qualifiedGraphName: QualifiedGraphName): CAPSPhysicalOperator =
    operators.ProjectExternalGraph(in, name, qualifiedGraphName)

  override def planProjectPatternGraph(
    in: CAPSPhysicalOperator,
    toCreate: Set[ConstructedEntity],
    name: String,
    schema: VerifiedSchema,
    header: RecordHeader): CAPSPhysicalOperator = operators.ProjectPatternGraph(in, toCreate, name, schema, header)

  override def planAggregate(in: CAPSPhysicalOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): CAPSPhysicalOperator = operators.Aggregate(in, aggregations, group, header)

  override def planFilter(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.Filter(in, expr, header)

  override def planValueJoin(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    predicates: Set[Equals],
    header: RecordHeader): CAPSPhysicalOperator = operators.ValueJoin(lhs, rhs, predicates, header)

  override def planDistinct(in: CAPSPhysicalOperator, fields: Set[Var]): CAPSPhysicalOperator =
    operators.Distinct(in, fields)

  override def planStartFromUnit(graph: LogicalExternalGraph): CAPSPhysicalOperator =
    operators.StartFromUnit(graph)

  override def planExpandSource(
    first: CAPSPhysicalOperator,
    second: CAPSPhysicalOperator,
    third: CAPSPhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader,
    removeSelfRelationships: Boolean): CAPSPhysicalOperator = operators.ExpandSource(
    first, second, third, source, rel, target, header, removeSelfRelationships)

  override def planUnion(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator): CAPSPhysicalOperator =
    operators.Union(lhs, rhs)

  override def planExpandInto(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.ExpandInto(lhs, rhs, source, rel, target, header)

  override def planInitVarExpand(
    in: CAPSPhysicalOperator,
    source: Var,
    edgeList: Var,
    target: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.InitVarExpand(in, source, edgeList, target, header)

  override def planBoundedVarExpand(
    first: CAPSPhysicalOperator,
    second: CAPSPhysicalOperator,
    third: CAPSPhysicalOperator,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean): CAPSPhysicalOperator = operators.BoundedVarExpand(
    first, second, third, rel, edgeList, target, initialEndNode, lower, upper, direction, header, isExpandInto)

  override def planOptional(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeader): CAPSPhysicalOperator =
    operators.Optional(lhs, rhs, header)

  override def planExistsSubQuery(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    targetField: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.ExistsSubQuery(lhs, rhs, targetField, header)

  override def planOrderBy(
    in: CAPSPhysicalOperator,
    sortItems: Seq[SortItem[Expr]],
    header: RecordHeader): CAPSPhysicalOperator = operators.OrderBy(in, sortItems)

  override def planSkip(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.Skip(in, expr, header)

  override def planLimit(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.Limit(in, expr, header)
}
