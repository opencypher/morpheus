package org.opencypher.spark.prototype.impl.logical

import org.opencypher.spark.prototype.api.expr.{EndNode, Expr, StartNode, Var}
import org.opencypher.spark.prototype.api.ir.SolvedQueryModel
import org.opencypher.spark.prototype.api.ir.pattern.EveryNode

import scala.language.implicitConversions

sealed trait LogicalOperator {
  def signature: Signature
  def isLeaf = false
  def solved: SolvedQueryModel[Expr]
}

case object Signature {
  def of(items: Item*) = Signature(items.toSet)
  val empty = Signature(Set.empty)
}

// TODO: Types
// TODO: Overlaps
final case class Signature(items: Set[Item]) {
  def ++(other: Signature) = copy(items = items ++ other.items)

  def apply(e: Expr) = items.find(_.contains(e)).get

  def withExpr(e: Expr) = if (items.exists(_.contains(e))) this else withItem(e)

  def withItem(item: Item) = copy(items = items + item)
  def withoutItem(item: Item) = copy(items = items - item)

  def selectItems(exprs: Set[Expr]) = copy(items = exprs.map(apply).toSet)

  def extendItemFor(existing: Expr)(extension: Expr) = {
    val item = apply(existing)
    updateItem(item)(_ + extension)
  }

  def updateItem(item: Item)(f: Item => Item) = {
    copy(items - item + f(item))
  }
}

case object Item {
  implicit def of(expr: Expr): Item = Item(Set(expr))
  def apply(exprs: Expr*): Item = Item(exprs.toSet)
}

// TODO: Non-Empty!
final case class Item(exprs: Set[Expr]) {
  def +(e: Expr): Item = copy(exprs = exprs + e)
  def contains(e: Expr) = exprs.contains(e)
}

sealed trait StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator
}

sealed trait LogicalLeafOperator extends LogicalOperator {
  override def isLeaf = true
}

final case class NodeScan(node: Var, nodeDef: EveryNode)
                         (override val solved: SolvedQueryModel[Expr]) extends LogicalLeafOperator {
  override def signature = Signature.empty.withExpr(node)
}

final case class Filter(expr: Expr, in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def signature = in.signature
}

sealed trait ExpandOperator extends StackingLogicalOperator {
  def source: Var
  def rel: Var
  def target: Var
}

final case class ExpandSource(source: Var, rel: Var, target: Var, in: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override def signature = in.signature.extendItemFor(source)(StartNode(rel)) withItem rel withItem Item(EndNode(rel), target)
}

final case class ExpandTarget(source: Var, rel: Var, target: Var, in: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override def signature = in.signature.extendItemFor(target)(EndNode(rel)) withItem rel withItem Item(StartNode(rel), source)
}

final case class Project(e: Expr, in: LogicalOperator)
                        (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def signature = in.signature.withExpr(e)
}

final case class Select(fields: Seq[(Expr, String)], in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {
  override def signature = in.signature.selectItems(fields.map(_._1).toSet)
}
