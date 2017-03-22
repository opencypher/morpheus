package org.opencypher.spark.prototype.impl.physical

import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.ir.SolvedQueryModel
import org.opencypher.spark.prototype.api.ir.pattern.EveryNode
import org.opencypher.spark.prototype.api.record.RecordHeader

sealed trait PhysicalOperator {
  def isLeaf = false

  def header: RecordHeader
}

sealed trait StackingPhysicalOperator extends PhysicalOperator {
  def in: PhysicalOperator
}

sealed trait PhysicalLeafOperator extends PhysicalOperator

final case class NodeScan(node: Var, nodeDef: EveryNode)(override val header: RecordHeader)
  extends PhysicalLeafOperator {
}

final case class Filter(expr: Expr, in: PhysicalOperator, header: RecordHeader = RecordHeader.empty)
  extends StackingPhysicalOperator

final case class Select(fields: Set[Var], in: PhysicalOperator, header: RecordHeader = RecordHeader.empty)
  extends StackingPhysicalOperator {
}
