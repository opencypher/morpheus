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
import org.opencypher.okapi.impl.exception.IllegalStateException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer
import org.opencypher.okapi.logical.impl.{Direction, LogicalGraph}
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.table._

import scala.annotation.tailrec

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private implicit val typeVectorMonoid: Monoid[Vector[CypherType]] {
    def empty: Vector[CypherType]

    def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType]
  } = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty

    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def cartesianProduct(lhs: FlatOperator, rhs: FlatOperator): CartesianProduct = {
    val header = lhs.header ++ rhs.header
    CartesianProduct(lhs, rhs, header)
  }

  def select(vars: List[Var], in: FlatOperator): Select = {
    Select(vars, in, in.header.select(vars: _*))
  }

  def returnGraph(in: FlatOperator): ReturnGraph = {
    ReturnGraph(in)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    in.header

    //    expr match {
    //      case HasLabel(n, label) =>
    //        in.header.contents.map { c =>
    //
    //        }
    //      case _ => in.header
    //    }

    // TODO: Should replace SlotContent expressions with detailed type of entity
    // TODO: Should reduce width of header due to more label information

    Filter(expr, in, in.header)
  }

  def distinct(fields: Set[Var], in: FlatOperator): Distinct = {
    Distinct(fields, in, in.header)
  }

  /**
    * This acts like a leaf operator even though it has an ancestor in the tree.
    * That means that it will discard any incoming fields from the ancestor header (assumes it is empty)
    */
  def nodeScan(node: Var, prev: FlatOperator): NodeScan = {
    NodeScan(node, prev, prev.sourceGraph.schema.headerForNode(node))
  }

  def relationshipScan(rel: Var, prev: FlatOperator): RelationshipScan = {
    RelationshipScan(rel, prev, prev.sourceGraph.schema.headerForRelationship(rel))
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = {
    t match {
      case l: CTList => relTypeFromList(l.elementType)
      case r: CTRelationship => r.types
      case _ => throw IllegalStateException(s"Required CTList or CTRelationship, but got $t")
    }
  }

  def varLengthRelationshipScan(relationshipList: Var, prev: FlatOperator): RelationshipScan = {
    val types = relTypeFromList(relationshipList.cypherType)
    val edge = FreshVariableNamer(relationshipList.name + "extended", CTRelationship(types, relationshipList.cypherType.graph))
    relationshipScan(edge, prev)
  }

  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    val newHeader = in.header.select(group).withExprs(aggregations.map(_._1))
    Aggregate(aggregations, group, in, newHeader)
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): Unwind = {
    Unwind(list, item, in, in.header.withExpr(item))
  }

  def project(projectExpr: (Expr, Option[Var]), in: FlatOperator): FlatOperator = {
    val (expr, maybeAlias) = projectExpr
    val updatedHeader = in.header.withExpr(expr)
    val containsExpr = in.header.contains(expr)

    maybeAlias match {
      case Some(alias) if containsExpr => Alias(expr, alias, in, updatedHeader.withAlias(expr as alias))
      case Some(alias) => Project(expr, in, updatedHeader.withAlias(expr as alias))
      case None => Project(expr, in, updatedHeader)
    }
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
    val relHeader = schema.headerForRelationship(rel)
    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header
    Expand(source, rel, direction, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def expandInto(
    source: Var,
    rel: Var,
    target: Var,
    direction: Direction,
    schema: Schema,
    sourceOp: FlatOperator
  ): FlatOperator = {
    val relHeader = schema.headerForRelationship(rel)
    val expandHeader = sourceOp.header ++ relHeader
    ExpandInto(source, rel, target, direction, sourceOp, expandHeader, relHeader)
  }

  def valueJoin(
    lhs: FlatOperator,
    rhs: FlatOperator,
    predicates: Set[org.opencypher.okapi.ir.api.expr.Equals]
  ): FlatOperator = {
    ValueJoin(lhs, rhs, predicates, lhs.header ++ rhs.header)
  }

  def planFromGraph(graph: LogicalGraph, prev: FlatOperator): FromGraph = {
    FromGraph(graph, prev)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    EmptyRecords(prev, RecordHeader.from(fields))
  }

  def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
    Start(graph, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val header = in.header.withExprs(edgeList, endNodeId)
    InitVarExpand(source, edgeList, endNodeId, in, header)
  }

  def boundedVarExpand(
    edge: Var,
    edgeList: Var,
    target: Var,
    direction: Direction,
    lower: Int,
    upper: Int,
    sourceOp: InitVarExpand,
    edgeOp: FlatOperator,
    targetOp: FlatOperator,
    isExpandInto: Boolean
  ): FlatOperator = {

    val initHeader = sourceOp.in.header.withExpr(edgeList)
    val header = initHeader ++ targetOp.header
    BoundedVarExpand(edge, edgeList, target, direction, lower, upper, sourceOp, edgeOp, targetOp, header, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, rhs.header)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    val header = lhs.header.withExpr(expr).withAlias(expr as expr.targetField)
    ExistsSubQuery(expr.targetField, lhs, rhs, header)
  }

  def orderBy(sortItems: Seq[SortItem[Expr]], sourceOp: FlatOperator): FlatOperator = {
    OrderBy(sortItems, sourceOp, sourceOp.header)
  }

  def skip(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Skip(expr, sourceOp, sourceOp.header)
  }

  def limit(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Limit(expr, sourceOp, sourceOp.header)
  }
}
