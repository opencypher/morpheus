package org.opencypher.spark.prototype.impl.logical

import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.SolvedQueryModel
import org.opencypher.spark.prototype.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordHeader}

import scala.language.implicitConversions

sealed trait LogicalOperator {
  def isLeaf = false
  def solved: SolvedQueryModel[Expr]
  def signature: RecordHeader
}

sealed trait StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator
}

sealed trait LogicalLeafOperator extends LogicalOperator {
  override def isLeaf = true
}

final case class NodeScan(node: Var, nodeDef: EveryNode, signature: RecordHeader = RecordHeader.empty)
                         (override val solved: SolvedQueryModel[Expr]) extends LogicalLeafOperator {
}

final case class Filter(expr: Expr, in: LogicalOperator, signature: RecordHeader = RecordHeader.empty)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
}

sealed trait ExpandOperator extends StackingLogicalOperator {
  def source: Var
  def rel: Var
  def target: Var
}

/*
          implicit graph

 source                  target?
  1                        4
  2                        5
  3                        6



 source  graph target
  1
  2
  3

 */

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var, in: LogicalOperator, signature: RecordHeader = RecordHeader.empty)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {
}

final case class ExpandTarget(source: Var, rel: Var, target: Var, in: LogicalOperator, signature: RecordHeader = RecordHeader.empty)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {
}

final case class Project(it: ProjectedSlotContent, in: LogicalOperator, signature: RecordHeader = RecordHeader.empty)
                        (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
}

final case class Select(fields: Set[Var], in: LogicalOperator, signature: RecordHeader = RecordHeader.empty)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
}
