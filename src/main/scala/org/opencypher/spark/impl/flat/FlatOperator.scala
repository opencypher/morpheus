package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.impl.logical.{EmptyGraph, GraphSource, LogicalGraph, NamedLogicalGraph}

sealed trait FlatOperator {
  def isLeaf = false

  def header: RecordHeader

  def inGraph: LogicalGraph
  def outGraph: NamedLogicalGraph
}

sealed trait StackingFlatOperator extends FlatOperator {
  def in: FlatOperator

  override def inGraph = in.outGraph
  override def outGraph = in.outGraph
}

sealed trait FlatLeafOperator extends FlatOperator

final case class NodeScan(node: Var, nodeDef: EveryNode, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
}

final case class Filter(expr: Expr, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator

final case class Select(fields: Set[Var], in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
}

final case class Alias(expr: Expr, alias: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
}

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
}

final case class LoadGraph(outGraph: NamedLogicalGraph, source: GraphSource) extends FlatLeafOperator {
  override val inGraph = EmptyGraph
  override val header = RecordHeader.empty
}
