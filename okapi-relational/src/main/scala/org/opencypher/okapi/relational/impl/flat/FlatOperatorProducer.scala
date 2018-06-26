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
package org.opencypher.okapi.relational.impl.flat

import cats.Monoid
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.{Direction, LogicalGraph}
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.table._

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private implicit val typeVectorMonoid: Monoid[Vector[CypherType]] {
    def empty: Vector[CypherType]

    def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType]
  } = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty

    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def cartesianProduct(lhs: FlatOperator, rhs: FlatOperator): CartesianProduct = {
    CartesianProduct(lhs, rhs)
  }

  def select(vars: List[Var], in: FlatOperator): Select = {
    Select(vars, in)
  }

  def returnGraph(in: FlatOperator): ReturnGraph = {
    ReturnGraph(in)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    Filter(expr, in)
  }

  def distinct(fields: Set[Var], in: FlatOperator): Distinct = {
    Distinct(fields, in)
  }

  /**
    * This acts like a leaf operator even though it has an ancestor in the tree.
    * That means that it will discard any incoming fields from the ancestor header (assumes it is empty)
    */
  def nodeScan(node: Var, prev: FlatOperator): NodeScan = {
    NodeScan(node, prev)
  }

  def relationshipScan(rel: Var, prev: FlatOperator): RelationshipScan = {
    RelationshipScan(rel, prev)
  }

  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    Aggregate(aggregations, group, in)
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): WithColumn = {
    val explodeExpr = Explode(list)(item.cypherType)
    WithColumn(explodeExpr as item, in)
  }

  def project(projectExpr: (Expr, Option[Var]), in: FlatOperator): FlatOperator = {
    Project(projectExpr, in)
  }

  def expand(
    source: Var,
    rel: Var,
    direction: Direction,
    target: Var,
    schema: Schema,
    sourceOp: FlatOperator,
    targetOp: FlatOperator
  ): FlatOperator = {
    Expand(source, rel, direction, target, sourceOp, targetOp)
  }

  def expandInto(
    source: Var,
    rel: Var,
    target: Var,
    direction: Direction,
    schema: Schema,
    sourceOp: FlatOperator
  ): FlatOperator = {
    ExpandInto(source, rel, target, direction, sourceOp)
  }

  def valueJoin(
    lhs: FlatOperator,
    rhs: FlatOperator,
    predicates: Set[org.opencypher.okapi.ir.api.expr.Equals]
  ): FlatOperator = {
    ValueJoin(lhs, rhs, predicates)
  }

  def planFromGraph(graph: LogicalGraph, prev: FlatOperator): FromGraph = {
    FromGraph(graph, prev)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    EmptyRecords(prev, RecordHeader.from(fields))
  }

  def planStart(graph: LogicalGraph): Start = {
    Start(graph)
  }

  def boundedVarExpand(
    source: Var,
    list: Var,
    edgeScan: Var,
    target: Var,
    direction: Direction,
    lower: Int,
    upper: Int,
    sourceOp: FlatOperator,
    edgeScanOp: FlatOperator,
    targetOp: FlatOperator,
    isExpandInto: Boolean
  ): FlatOperator = {
    BoundedVarExpand(source, list, edgeScan, target, direction, lower, upper, sourceOp, edgeScanOp, targetOp, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    ExistsSubQuery(expr.targetField, lhs, rhs)
  }

  def orderBy(sortItems: Seq[SortItem[Expr]], sourceOp: FlatOperator): FlatOperator = {
    OrderBy(sortItems, sourceOp)
  }

  def skip(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Skip(expr, sourceOp)
  }

  def limit(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Limit(expr, sourceOp)
  }
}
