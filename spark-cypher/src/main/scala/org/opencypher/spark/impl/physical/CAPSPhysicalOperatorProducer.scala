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
  constructedGraphPlans: collection.mutable.Map[QualifiedGraphName, CAPSPhysicalOperator]
) extends PhysicalPlannerContext[DataFrameTable, CAPSPhysicalOperator, CAPSRecords]

object CAPSPhysicalPlannerContext {
  def from(
    catalog: QueryCatalog,
    inputRecords: CAPSRecords,
    parameters: CypherMap)(implicit session: CAPSSession): PhysicalPlannerContext[DataFrameTable, CAPSPhysicalOperator, CAPSRecords] = {
    CAPSPhysicalPlannerContext(session, catalog, inputRecords, parameters, collection.mutable.Map.empty)
  }
}

final class CAPSPhysicalOperatorProducer(implicit context: CAPSRuntimeContext)
  extends PhysicalOperatorProducer[DataFrameTable, CAPSPhysicalOperator, CAPSRecords, CAPSGraph, CAPSRuntimeContext] {

  override def planCartesianProduct(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator): CAPSPhysicalOperator = operators.Join(lhs, rhs, Seq.empty, CrossJoin)

  override def planDrop(
    in: CAPSPhysicalOperator,
    dropFields: Set[Expr]
  ): CAPSPhysicalOperator = operators.Drop(in, dropFields)

  override def planRenameColumns(
    in: CAPSPhysicalOperator,
    renameExprs: Map[Expr, String]
  ): CAPSPhysicalOperator = operators.RenameColumns(in, renameExprs)

  override def planSelect(in: CAPSPhysicalOperator, exprs: List[Expr]): CAPSPhysicalOperator =
    operators.Select(in, exprs)

  override def planReturnGraph(in: CAPSPhysicalOperator): CAPSPhysicalOperator = {
    operators.ReturnGraph(in)
  }

  override def planEmptyRecords(in: CAPSPhysicalOperator, fields: Set[Var]): CAPSPhysicalOperator =
    operators.EmptyRecords(in, fields)

  override def planStart(
    qgnOpt: Option[QualifiedGraphName] = None,
    in: Option[CAPSRecords] = None): CAPSPhysicalOperator =
    operators.Start(qgnOpt.getOrElse(context.session.emptyGraphQgn), in)

  // TODO: Make catalog usage consistent between Start/FROM GRAPH
  override def planFromGraph(in: CAPSPhysicalOperator, g: LogicalCatalogGraph): CAPSPhysicalOperator =
    operators.FromGraph(in, g)

  override def planNodeScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var): CAPSPhysicalOperator = operators.NodeScan(in, v)

  override def planRelationshipScan(
    in: CAPSPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var): CAPSPhysicalOperator = operators.RelationshipScan(in, v)

  override def planAliases(in: CAPSPhysicalOperator, aliases: Seq[AliasExpr]): CAPSPhysicalOperator =
    operators.Alias(in, aliases)

  override def planAdd(in: CAPSPhysicalOperator, expr: Expr): CAPSPhysicalOperator =
    operators.Add(in, expr)

  override def planAddInto(in: CAPSPhysicalOperator, from: Expr, to: Expr): CAPSPhysicalOperator =
    operators.AddInto(in, from, to)

  override def planConstructGraph(
    in: CAPSPhysicalOperator,
    onGraph: CAPSPhysicalOperator,
    construct: LogicalPatternGraph): CAPSPhysicalOperator = {
    operators.ConstructGraph(in, onGraph, construct)
  }

  override def planAggregate(in: CAPSPhysicalOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)]): CAPSPhysicalOperator = operators.Aggregate(in, aggregations, group)

  override def planFilter(in: CAPSPhysicalOperator, expr: Expr): CAPSPhysicalOperator =
    operators.Filter(in, expr)

  override def planJoin(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    joinColumns: Seq[(Expr, Expr)],
    joinType: JoinType): CAPSPhysicalOperator = {

    operators.Join(lhs, rhs, joinColumns, joinType)
  }

  override def planDistinct(in: CAPSPhysicalOperator, fields: Set[Var]): CAPSPhysicalOperator =
    operators.Distinct(in, fields)

  override def planTabularUnionAll(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator): CAPSPhysicalOperator =
    operators.TabularUnionAll(lhs, rhs)

  override def planOrderBy(
    in: CAPSPhysicalOperator,
    sortItems: Seq[SortItem[Expr]]): CAPSPhysicalOperator = operators.OrderBy(in, sortItems)

  override def planSkip(in: CAPSPhysicalOperator, expr: Expr): CAPSPhysicalOperator =
    operators.Skip(in, expr)

  override def planLimit(in: CAPSPhysicalOperator, expr: Expr): CAPSPhysicalOperator =
    operators.Limit(in, expr)

  override def planGraphUnionAll(graphs: List[CAPSPhysicalOperator], qgn: QualifiedGraphName):
    CAPSPhysicalOperator = {
    operators.GraphUnionAll(graphs, qgn)
  }
}
