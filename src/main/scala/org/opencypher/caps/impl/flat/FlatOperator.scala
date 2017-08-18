/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.caps.api.expr.{Aggregator, Expr, Var}
import org.opencypher.caps.api.ir.block.SortItem
import org.opencypher.caps.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.caps.api.record.{OpaqueField, RecordHeader}
import org.opencypher.caps.impl.logical.{EmptyGraph, GraphSource, LogicalGraph, NamedLogicalGraph}

sealed trait FlatOperator {
  def isLeaf = false

  def header: RecordHeader

  def inGraph: LogicalGraph
  def outGraph: NamedLogicalGraph
}

sealed trait BinaryFlatOperator extends FlatOperator {
  def lhs: FlatOperator
  def rhs: FlatOperator

  override def inGraph = lhs.outGraph
  override def outGraph = lhs.outGraph
}

sealed trait TernaryFlatOperator extends FlatOperator {
  def first:  FlatOperator
  def second: FlatOperator
  def third:  FlatOperator

  override def inGraph = first.outGraph
  override def outGraph = first.outGraph
}

sealed trait StackingFlatOperator extends FlatOperator {
  def in: FlatOperator

  override def inGraph = in.outGraph
  override def outGraph = in.outGraph
}

sealed trait FlatLeafOperator extends FlatOperator

final case class NodeScan(node: Var, nodeDef: EveryNode, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class EdgeScan(edge: Var, edgeDef: EveryRelationship, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Filter(expr: Expr, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Select(fields: IndexedSeq[Var], in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Project(expr: Expr, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator

final case class Aggregate(to: Var, agg: Aggregator, group: Set[Var], in: FlatOperator, header: RecordHeader) extends StackingFlatOperator

final case class Alias(expr: Expr, alias: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Optional(lhs: FlatOperator, rhs: FlatOperator, lhsHeader: RecordHeader, rhsHeader: RecordHeader)
  extends BinaryFlatOperator {

  override def header: RecordHeader = rhsHeader
}

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                              sourceOp: FlatOperator, targetOp: FlatOperator, header: RecordHeader, relHeader: RecordHeader)
  extends BinaryFlatOperator {

  override def lhs = sourceOp
  override def rhs = targetOp
}

final case class ExpandInto(source: Var, rel: Var, types: EveryRelationship, target: Var, sourceOp: FlatOperator,
                            header: RecordHeader, relHeader: RecordHeader)
  extends StackingFlatOperator {

  override def in: FlatOperator = sourceOp
}

final case class InitVarExpand(source: Var, edgeList: Var, endNode: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class BoundedVarExpand(rel: Var, edgeList: Var, target: Var, lower: Int, upper: Int,
                                  sourceOp: InitVarExpand, relOp: FlatOperator, targetOp: FlatOperator, header: RecordHeader)
  extends TernaryFlatOperator {

  override def first  = sourceOp
  override def second = relOp
  override def third  = targetOp
}

final case class OrderBy(sortItems: Seq[SortItem[Expr]], in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Skip(expr: Expr, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Limit(expr: Expr, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Start(outGraph: NamedLogicalGraph, source: GraphSource, fields: Set[Var]) extends FlatLeafOperator {
  override val inGraph = EmptyGraph
  override val header = RecordHeader.from(fields.map(OpaqueField).toSeq: _*)
}
