package org.opencypher.spark.impl.prototype.logical

import org.opencypher.spark.impl.prototype._

import scala.collection.immutable.SortedSet

sealed trait LogicalOperator {
  def signature: Signature
  def isLeaf = false
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

final case class NodeScan(node: Var, nodeDef: AnyNode) extends LogicalLeafOperator {
  override def signature = Signature.empty
}

final case class Filter(expr: Expr, in: LogicalOperator) extends StackingLogicalOperator {
  override def signature = in.signature
}

sealed trait ExpandOperator extends StackingLogicalOperator {
  def node: Var
  def rel: Var
}

final case class ExpandSource(node: Var, rel: Var, in: LogicalOperator)
  extends ExpandOperator {

  override def signature = in.signature.extendItemFor(node)(StartNode(rel)) withItem rel withItem EndNode(rel)
}

final case class ExpandTarget(node: Var, rel: Var, in: LogicalOperator)
  extends ExpandOperator {

  override def signature = in.signature.extendItemFor(node)(EndNode(rel)) withItem rel withItem StartNode(rel)
}

final case class Project(e: Expr, in: LogicalOperator) extends StackingLogicalOperator {
  override def signature = in.signature.withExpr(e)
}

final case class Select(fields: SortedSet[(Expr, String)], in: LogicalOperator) extends StackingLogicalOperator {
  override def signature = in.signature.selectItems(fields.map(_._1))
}
