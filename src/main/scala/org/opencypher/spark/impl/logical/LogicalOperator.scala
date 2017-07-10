package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.SolvedQueryModel
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.ProjectedSlotContent
import org.opencypher.spark.api.schema.Schema

import scala.language.implicitConversions

sealed trait LogicalOperator {
  def isLeaf = false
  def solved: SolvedQueryModel[Expr]

  def inGraph: LogicalGraph
  def outGraph: NamedLogicalGraph
}

sealed trait LogicalGraph {
  def schema: Schema
}

case object EmptyGraph extends LogicalGraph {
  override val schema = Schema.empty
}

final case class NamedLogicalGraph(name: String, schema: Schema) extends LogicalGraph

sealed trait StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator

  override def outGraph = in.outGraph
  override def inGraph = in.outGraph
}

sealed trait BinaryLogicalOperator extends LogicalOperator {
  def lhs: LogicalOperator
  def rhs: LogicalOperator

  // Always follow the left-hand side
  override def outGraph = lhs.outGraph
  override def inGraph = lhs.outGraph
}

sealed trait LogicalLeafOperator extends LogicalOperator {
  override def isLeaf = true
}

final case class NodeScan(node: Var, nodeDef: EveryNode, in: LogicalOperator)
                         (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator

final case class Filter(expr: Expr, in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator

sealed trait ExpandOperator extends BinaryLogicalOperator {
  def source: Var
  def rel: Var
  def target: Var

  def sourceOp: LogicalOperator
  def targetOp: LogicalOperator
}

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                              sourceOp: LogicalOperator, targetOp: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override def lhs = sourceOp
  override def rhs = targetOp
}

final case class ExpandTarget(source: Var, rel: Var, target: Var,
                              sourceOp: LogicalOperator, targetOp: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override def lhs = targetOp
  override def rhs = sourceOp
}

final case class Project(it: ProjectedSlotContent, in: LogicalOperator)
                        (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator

final case class Select(fields: IndexedSeq[Var], in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator

final case class LoadGraph(outGraph: NamedLogicalGraph, source: GraphSource, fields: Set[Var])
                          (override val solved: SolvedQueryModel[Expr]) extends LogicalLeafOperator {
  override val inGraph = EmptyGraph
}

sealed trait GraphSource
case object DefaultGraphSource extends GraphSource
