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

  def select(fields: List[Var], in: FlatOperator): Select = {
    val fieldContents = fields.map { field =>
      in.header.slotFor(field).content
    }

    val finalContents = fieldContents ++ fields.flatMap(in.header.childSlots).map(_.content)

    val nextHeader = IRecordHeader.empty.addContents(finalContents)

    Select(fields, in, nextHeader)
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
    val header = IRecordHeader.nodeFromSchema(node, prev.sourceGraph.schema)

    new NodeScan(node, prev, header)
  }

  def edgeScan(edge: Var, prev: FlatOperator): EdgeScan = {
    val edgeHeader = IRecordHeader.relationshipFromSchema(edge, prev.sourceGraph.schema)

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
    val newHeader = IRecordHeader.empty
      .addContents(group.flatMap(in.header.selfWithChildren).map(_.content).toSeq ++ aggregations.map(agg => OpaqueField(agg._1)))

    Aggregate(aggregations, group, in, newHeader)
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): Unwind = {
    val header = in.header.addContent(OpaqueField(item))
    Unwind(list, item, in, header)
  }

  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val existing = in.header.contains(it)
    val newHeader = in.header.addContent(it)

    // TODO: we had a noop before when `it` has been found in the header.
    // we now plan a project that just forwards the DF if the column already exists

    if (existing) {
      // TODO: noop
      Alias(it.expr, it.alias.get, in, newHeader)
    } else {
      Project(it.expr, in, newHeader)
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
    val relHeader = IRecordHeader.relationshipFromSchema(rel, schema)

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
    val relHeader = IRecordHeader.relationshipFromSchema(rel, schema)

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

  def planFromGraph(graph: LogicalGraph, prev: FlatOperator) = {
    FromGraph(graph, prev)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    val header = IRecordHeader.from(fields.map(OpaqueField).toSeq: _*)
    EmptyRecords(prev, header)
  }

  def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
    Start(graph, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val header = in.header.addContents(Seq(OpaqueField(edgeList), OpaqueField(endNodeId)))
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

    val initHeader = sourceOp.in.header.addContent(OpaqueField(edgeList))
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, direction, lower, upper, sourceOp, edgeOp, targetOp, header, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, rhs.header)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    val header = lhs.header.addContent(ProjectedField(expr.targetField, expr))
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
