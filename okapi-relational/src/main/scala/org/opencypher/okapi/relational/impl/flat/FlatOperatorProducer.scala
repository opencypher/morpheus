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

import cats.Monoid
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalStateException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer
import org.opencypher.okapi.logical.impl.{Direction, LogicalGraph}
import org.opencypher.okapi.relational.impl.exception.RecordHeaderException
import org.opencypher.okapi.relational.impl.table.RecordHeader._
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

  def select(orderedFields: List[Var], in: FlatOperator): Select = {
    require(orderedFields.toSet.subsetOf(in.header.fields))
    val updatedHeader = in.header.selectFields(orderedFields.toSet)
    Select(orderedFields, in, updatedHeader)
  }

  def returnGraph(in: FlatOperator): ReturnGraph = {
    ReturnGraph(in)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
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
    val header = RecordHeader.forNode(node, prev.sourceGraph.schema)

    new NodeScan(node, prev, header)
  }

  def edgeScan(edge: Var, prev: FlatOperator): EdgeScan = {
    val edgeHeader = RecordHeader.forRelationship(edge, prev.sourceGraph.schema)

    EdgeScan(edge, prev, edgeHeader)
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = {
    t match {
      case l: CTList => relTypeFromList(l.elementType)
      case r: CTRelationship => r.types
      case _ => throw IllegalStateException(s"Required CTList or CTRelationship, but got $t")
    }
  }

  def varLengthEdgeScan(edgeList: Var, prev: FlatOperator): EdgeScan = {
    val types = relTypeFromList(edgeList.cypherType)
    val edge = FreshVariableNamer(edgeList.name + "extended", CTRelationship(types, edgeList.cypherType.graph))
    edgeScan(edge, prev)
  }

  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    val newHeaderContents = group.flatMap(in.header.selectField).map(_.expr).toList ++ aggregations.map(_._1)

    Aggregate(aggregations, group, in, RecordHeader(newHeaderContents: _*))
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): Unwind = {
    Unwind(list, item, in, in.header.withField(item))
  }

  def project(expr: Expr, maybeField: Option[Var], in: FlatOperator): FlatOperator = {
    def header = in.header
    header.getExprFor(expr) match {
      case Some((existingExpr, fields)) => Project(existingExpr, in, maybeField.map(header.withField(_)).getOrElse(header))
      case None => Project(expr, in, header.withMapping(expr -> maybeField.toSet))
    }
  }

  def expand(
    source: Var,
    rel: Var,
    direction: Direction,
    target: Var,
    schema: Schema,
    sourceOp: FlatOperator,
    targetOp: FlatOperator): FlatOperator = {
    val relHeader = RecordHeader.forRelationship(rel, schema)

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    Expand(source, rel, direction, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def expandInto(source: Var, rel: Var, target: Var, direction: Direction, schema: Schema, sourceOp: FlatOperator): FlatOperator = {
    val relHeader = RecordHeader.forRelationship(rel, schema)

    val expandHeader = sourceOp.header ++ relHeader

    ExpandInto(source, rel, target, direction, sourceOp, expandHeader, relHeader)
  }

  def valueJoin(
    lhs: FlatOperator,
    rhs: FlatOperator,
    predicates: Set[org.opencypher.okapi.ir.api.expr.Equals]): FlatOperator = {
    ValueJoin(lhs, rhs, predicates, lhs.header ++ rhs.header)
  }

  def planFromGraph(graph: LogicalGraph, prev: FlatOperator) = {
    FromGraph(graph, prev)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    EmptyRecords(prev, RecordHeader.empty.withFields(fields.toSeq: _*))
  }

  def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
    Start(graph, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val header = in.header.withExpressions(edgeList, endNodeId)

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
    isExpandInto: Boolean): FlatOperator = {

    val initHeader = sourceOp.in.header.withField(edgeList)
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, direction, lower, upper, sourceOp, edgeOp, targetOp, header, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, rhs.header)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    val header = lhs.header.withMapping(expr -> Set(expr.targetField))

    def expressionExistsAlready: Boolean = lhs.header.contains(expr)

    if (expressionExistsAlready) {
      throw RecordHeaderException(s"Expression exists already in header: $expr")
    } else {
      ExistsSubQuery(expr.targetField, lhs, rhs, header)
    }
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
