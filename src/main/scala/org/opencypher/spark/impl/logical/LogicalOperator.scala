package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.SolvedQueryModel
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.ProjectedSlotContent

import scala.language.implicitConversions

sealed trait LogicalOperator {
  def isLeaf = false
  def solved: SolvedQueryModel[Expr]
  def graph: LogicalGraph

  // TODO: Make this better
  def passingOn: NamedLogicalGraph = graph.asInstanceOf[NamedLogicalGraph]
}

sealed trait LogicalGraph
case object IDontCareGraph extends LogicalGraph
final case class NamedLogicalGraph(name: String) extends LogicalGraph

sealed trait StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator
}

sealed trait LogicalLeafOperator extends LogicalOperator {
  override def isLeaf = true
}

final case class NodeScan(node: Var, nodeDef: EveryNode, in: LogicalOperator)
                         (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def graph = in.passingOn
}

final case class Filter(expr: Expr, in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def graph = in.passingOn
}

sealed trait ExpandOperator extends StackingLogicalOperator {
  def source: Var
  def rel: Var
  def target: Var
}

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var, in: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {
  override def graph = in.passingOn
}

final case class ExpandTarget(source: Var, rel: Var, target: Var, in: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {
  override def graph = in.passingOn
}

final case class Project(it: ProjectedSlotContent, in: LogicalOperator)
                        (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def graph = in.passingOn
}

final case class Select(fields: Set[Var], in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def graph = in.passingOn
}

final case class LoadGraph(graph: LogicalGraph, loaded: NamedLogicalGraph)
                          (override val solved: SolvedQueryModel[Expr]) extends LogicalLeafOperator {
  override def passingOn: NamedLogicalGraph = loaded
}
