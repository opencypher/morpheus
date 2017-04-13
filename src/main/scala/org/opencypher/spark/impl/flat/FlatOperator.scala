package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.RecordHeader

sealed trait FlatOperator {
  def isLeaf = false

  def header: RecordHeader
}

sealed trait StackingFlatOperator extends FlatOperator {
  def in: FlatOperator
}

sealed trait FlatLeafOperator extends FlatOperator

final case class NodeScan(node: Var, nodeDef: EveryNode, header: RecordHeader)
  extends FlatLeafOperator {
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
