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
package org.opencypher.caps.impl.flat

import cats.Monoid
import org.opencypher.caps.api.exception.{IllegalStateException, RecordHeaderException}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.record._
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.util.FreshVariableNamer
import org.opencypher.caps.logical.impl.{Direction, LogicalGraph}

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

  def select(fields: IndexedSeq[Var], graphs: Set[String], in: FlatOperator): Select = {
    val fieldContents = fields.map { field =>
      in.header.slotsFor(field).head.content
    }

    val finalContents = fieldContents ++ fields.flatMap(in.header.childSlots).map(_.content)

    val (nextHeader, _) = RecordHeader.empty.update(addContents(finalContents))

    Select(fields, graphs, in, nextHeader)
  }

  def removeAliases(toKeep: IndexedSeq[Var], in: FlatOperator): FlatOperator = {
    val renames = in.header.contents.collect {
      case pf @ ProjectedField(v, _: Property | _: HasLabel | _: HasType) if !toKeep.contains(v) =>
        pf -> ProjectedExpr(pf.expr)
    }

    if (renames.isEmpty) {
      in
    } else {
      val newHeaderContents = in.header.contents.map {
        case pf @ ProjectedField(v, _: Property | _: HasLabel | _: HasType) if !toKeep.contains(v) =>
          ProjectedExpr(pf.expr)
        case other =>
          other
      }

      val (header, _) = RecordHeader.empty.update(addContents(newHeaderContents.toSeq))

      RemoveAliases(renames, in, header)
    }
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
    val header = RecordHeader.nodeFromSchema(node, prev.sourceGraph.schema)

    new NodeScan(node, prev, header)
  }

  def edgeScan(edge: Var, prev: FlatOperator): EdgeScan = {
    val edgeHeader = RecordHeader.relationshipFromSchema(edge, prev.sourceGraph.schema)

    EdgeScan(edge, prev, edgeHeader)
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = {
    t match {
      case l: CTList         => relTypeFromList(l.elementType)
      case r: CTRelationship => r.types
      case _                 => throw IllegalStateException(s"Required CTList or CTRelationship, but got $t")
    }
  }

  def varLengthEdgeScan(edgeList: Var, prev: FlatOperator): EdgeScan = {
    val types = relTypeFromList(edgeList.cypherType)
    val edge = FreshVariableNamer(edgeList.name + "extended", CTRelationship(types))
    edgeScan(edge, prev)
  }

  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    val (newHeader, _) = RecordHeader.empty.update(
      addContents(
        group.flatMap(in.header.selfWithChildren).map(_.content).toSeq ++ aggregations.map(agg => OpaqueField(agg._1)))
    )

    Aggregate(aggregations, group, in, newHeader)
  }

  def unwind(list: Expr, item: Var, in: FlatOperator): Unwind = {
    val (header, _) = in.header.update(addContent(OpaqueField(item)))

    Unwind(list, item, in, header)
  }

  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      case _: Found[_]       => in
      case _: Replaced[_]    => Alias(it.expr, it.alias.get, in, newHeader)
      case _: Added[_]       => Project(it.expr, in, newHeader)
      case f: FailedToAdd[_] => throw RecordHeaderException(s"Slot already exists: ${f.conflict}")
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
    val relHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    Expand(source, rel, direction, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def expandInto(source: Var, rel: Var, target: Var, direction: Direction, schema: Schema, sourceOp: FlatOperator): FlatOperator = {
    val relHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val expandHeader = sourceOp.header ++ relHeader

    ExpandInto(source, rel, target, direction, sourceOp, expandHeader, relHeader)
  }

  def valueJoin(
      lhs: FlatOperator,
      rhs: FlatOperator,
      predicates: Set[org.opencypher.caps.ir.api.expr.Equals]): FlatOperator = {
    ValueJoin(lhs, rhs, predicates, lhs.header ++ rhs.header)
  }

  def planSetSourceGraph(graph: LogicalGraph, prev: FlatOperator) = {
    SetSourceGraph(graph, prev, prev.header)
  }

  def planEmptyRecords(fields: Set[Var], prev: FlatOperator): EmptyRecords = {
    val header = RecordHeader.from(fields.map(OpaqueField).toSeq: _*)
    EmptyRecords(prev, header)
  }

  def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
    Start(graph, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val (header, _) = in.header.update(addContents(Seq(OpaqueField(edgeList), OpaqueField(endNodeId))))

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

    val (initHeader, _) = sourceOp.in.header.update(addContent(OpaqueField(edgeList)))
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, direction, lower, upper, sourceOp, edgeOp, targetOp, header, isExpandInto)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, rhs.header)
  }

  def planExistsSubQuery(expr: ExistsPatternExpr, lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    val (header, status) = lhs.header.update(addContent(ProjectedField(expr.targetField, expr)))

    // TODO: record header should have sealed effects or be a map
    status match {
      case _: Added[_]       => ExistsSubQuery(expr.targetField, lhs, rhs, header)
      case f: FailedToAdd[_] => throw RecordHeaderException(s"Slot already exists: ${f.conflict}")
      case _                 => throw RecordHeaderException("Invalid RecordHeader update status.")
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
