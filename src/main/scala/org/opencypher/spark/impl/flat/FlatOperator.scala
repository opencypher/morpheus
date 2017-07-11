package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.impl.logical.{EmptyGraph, GraphSource, LogicalGraph, NamedLogicalGraph}

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

final case class Sanitize(in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Select(fields: IndexedSeq[Var], in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Project(expr: Expr, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator

final case class Alias(expr: Expr, alias: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                              sourceOp: FlatOperator, targetOp: FlatOperator, header: RecordHeader, relHeader: RecordHeader)
  extends BinaryFlatOperator {

  override def lhs = sourceOp
  override def rhs = targetOp
}

final case class InitVarExpand(source: Var, edgeList: Var, endNode: Var, in: FlatOperator,
                               header: RecordHeader)
  extends StackingFlatOperator

final case class BoundedVarExpand(rel: Var, edgeList: Var, target: Var, lower: Int, upper: Int,
                                  sourceOp: InitVarExpand, relOp: FlatOperator, targetOp: FlatOperator, header: RecordHeader)
  extends TernaryFlatOperator {

  override def first  = sourceOp
  override def second = relOp
  override def third  = targetOp
}

final case class Start(outGraph: NamedLogicalGraph, source: GraphSource, fields: Set[Var]) extends FlatLeafOperator {
  override val inGraph = EmptyGraph
  override val header = RecordHeader.from(fields.map(OpaqueField).toSeq: _*)
}
