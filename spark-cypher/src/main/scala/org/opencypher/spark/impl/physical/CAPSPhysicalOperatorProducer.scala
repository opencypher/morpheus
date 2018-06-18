/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.QueryCatalog
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.SparkCypherTable.DataFrameTable
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}

case class CAPSPhysicalPlannerContext(
  session: CAPSSession,
  catalog: QueryCatalog,
  inputRecords: CAPSRecords,
  parameters: CypherMap,
  constructedGraphPlans: collection.mutable.Map[QualifiedGraphName, CAPSPhysicalOperator]) extends PhysicalPlannerContext[CAPSPhysicalOperator, CAPSRecords]

object CAPSPhysicalPlannerContext {
  def from(
    catalog: QueryCatalog,
    inputRecords: CAPSRecords,
    parameters: CypherMap)(implicit session: CAPSSession): PhysicalPlannerContext[CAPSPhysicalOperator, CAPSRecords] = {
    CAPSPhysicalPlannerContext(session, catalog, inputRecords, parameters, collection.mutable.Map.empty)
  }
}

final class CAPSPhysicalOperatorProducer(implicit caps: CAPSSession)
  extends PhysicalOperatorProducer[DataFrameTable, CAPSPhysicalOperator, CAPSRecords, CAPSGraph, CAPSRuntimeContext] {

  override def planCartesianProduct(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    header: RecordHeader): CAPSPhysicalOperator = operators.CartesianProduct(lhs, rhs, header)

  override def planDrop(
    in: CAPSPhysicalOperator,
    dropFields: Set[Expr],
    header: RecordHeader
  ): CAPSPhysicalOperator = operators.DropColumns(in, dropFields, header)

  override def planRenameColumns(
    in: CAPSPhysicalOperator,
    renameExprs: Map[Expr, String],
    header: RecordHeader
  ): CAPSPhysicalOperator = operators.RenameColumns(in, renameExprs, header)

  override def planSelect(in: CAPSPhysicalOperator, exprs: List[Expr], header: RecordHeader): CAPSPhysicalOperator =
    operators.Select(in, exprs, header)

  override def planReturnGraph(in: CAPSPhysicalOperator): CAPSPhysicalOperator = {
    operators.ReturnGraph(in)
  }

  override def planEmptyRecords(in: CAPSPhysicalOperator, header: RecordHeader): CAPSPhysicalOperator =
    operators.EmptyRecords(in, header)

  override def planStart(
    qgnOpt: Option[QualifiedGraphName] = None,
    in: Option[CAPSRecords] = None,
    header: RecordHeader): CAPSPhysicalOperator =
    operators.Start(qgnOpt.getOrElse(caps.emptyGraphQgn), in, header)

  // TODO: Make catalog usage consistent between Start/FROM GRAPH
  override def planFromGraph(in: CAPSPhysicalOperator, g: LogicalCatalogGraph): CAPSPhysicalOperator =
    operators.FromGraph(in, g)

  override def planNodeScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.NodeScan(in, v, header)

  override def planRelationshipScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPSPhysicalOperator = operators.RelationshipScan(in, v, header)

  override def planAliases(in: CAPSPhysicalOperator, aliases: Seq[AliasExpr], header: RecordHeader): CAPSPhysicalOperator =
    operators.Alias(in, aliases, header)

  override def planAddColumn(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.AddColumn(in, expr, header)

  override def planCopyColumn(in: CAPSPhysicalOperator, from: Expr, to: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.CopyColumn(in, from, to, header)

  override def planConstructGraph(
    in: CAPSPhysicalOperator,
    onGraph: CAPSPhysicalOperator,
    construct: LogicalPatternGraph): CAPSPhysicalOperator = {
    operators.ConstructGraph(in, onGraph, construct)
  }

  override def planAggregate(in: CAPSPhysicalOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): CAPSPhysicalOperator = operators.Aggregate(in, aggregations, group, header)

  override def planFilter(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader): CAPSPhysicalOperator =
    operators.Filter(in, expr, header)

  override def planJoin(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    joinColumns: Seq[(Expr, Expr)],
    header: RecordHeader,
    joinType: JoinType): CAPSPhysicalOperator = {

    operators.Join(lhs, rhs, joinColumns, header, joinType)
  }

  override def planDistinct(in: CAPSPhysicalOperator, fields: Set[Var]): CAPSPhysicalOperator =
    operators.Distinct(in, fields)

  override def planTabularUnionAll(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator): CAPSPhysicalOperator =
    operators.TabularUnionAll(lhs, rhs)

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

  override def planGraphUnionAll(graphs: List[CAPSPhysicalOperator], qgn: QualifiedGraphName):
    CAPSPhysicalOperator = {
    operators.GraphUnionAll(graphs, qgn)
  }
}
