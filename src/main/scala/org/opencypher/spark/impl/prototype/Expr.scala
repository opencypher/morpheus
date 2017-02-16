package org.opencypher.spark.impl.prototype

import scala.annotation.tailrec

sealed trait Expr {
  def usedFields: Set[Field] = Set.empty
  def usedLabels: Set[LabelRef] = Set.empty
  def usedRelTypes: Set[RelTypeRef] = Set.empty
  def usedPropertyKeys: Set[PropertyKeyRef] = Set.empty
}

final case class Param(name: String) extends Expr

final case class Var(name: String) extends Expr

final case class Connected(source: Field, rel: Field, target: Field) extends Expr {
  override def usedFields = Set(source, rel, target)
}

trait FlatteningOpExprCompanion[T] {
  def apply(exprs: Expr*): T
  def apply(exprs: Set[Expr]): T
  def unapply(expr: Any): Option[Set[Expr]]
}

sealed abstract class FlatteningOpExpr(_exprs: Set[Expr]) extends Expr with Serializable with Product1[Set[Expr]] {

  val exprs: Set[Expr] =
    if (_exprs.isEmpty) throw new IllegalStateException("Attempt to construct empty Ands") else flatExpr(_exprs)

  override def _1: Set[Expr] = exprs

  override def equals(obj: scala.Any) = companion.unapply(obj).contains(exprs)
  override def hashCode() = exprs.hashCode() + hashPrime
  override def toString = s"$productPrefix(${exprs.mkString(", ")})"

  protected def companion: FlatteningOpExprCompanion[_]
  protected def hashPrime: Int

  @tailrec
  private def flatExpr(exprs: Set[Expr], result: Set[Expr] = Set.empty): Set[Expr] =
    if (exprs.isEmpty)
      result
    else {
      val expr = exprs.head
      val remaining = exprs.tail
      companion.unapply(expr) match  {
        case Some(moreExprs) => flatExpr(moreExprs ++ remaining, result)
        case None => flatExpr(remaining, result + expr)
      }
    }
}

object Ands extends FlatteningOpExprCompanion[Ands] {
  def apply(exprs: Expr*): Ands = Ands(exprs.toSet)
  def apply(exprs: Set[Expr]): Ands = new Ands(exprs)
  def unapply(expr: Any): Option[Set[Expr]] = expr match {
    case ands: Ands => Some(ands.exprs)
    case _ => None
  }
}

final class Ands(_exprs: Set[Expr]) extends FlatteningOpExpr(_exprs) {
  override def productPrefix = "Ands"
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Ands]
  override protected def companion: FlatteningOpExprCompanion[Ands] = Ands
  override protected def hashPrime: Int = 31
}

object Ors extends FlatteningOpExprCompanion[Ors] {
  def apply(exprs: Expr*): Ors = Ors(exprs.toSet)
  def apply(exprs: Set[Expr]): Ors = new Ors(exprs)
  def unapply(expr: Any): Option[Set[Expr]] = expr match {
    case ors: Ors => Some(ors.exprs)
    case _ => None
  }
}

final class Ors(_exprs: Set[Expr]) extends FlatteningOpExpr(_exprs) {
  override def productPrefix = "Ors"
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Ors]
  override protected def companion: FlatteningOpExprCompanion[Ors] = Ors
  override protected def hashPrime: Int = 61
}

final case class HasLabel(node: Expr, label: LabelRef) extends Expr
final case class HasType(rel: Expr, relType: RelTypeRef) extends Expr
final case class Equals(lhs: Expr, rhs: Expr) extends Expr

case class Property(m: Expr, key: PropertyKeyRef) extends Expr

sealed trait Literal[T] extends Expr {
  def v: T
}
final case class IntegerLit(v: Long) extends Literal[Long]
final case class StringLit(v: String) extends Literal[String]

sealed abstract class BoolLit(val v: Boolean) extends Literal[Boolean]
case object TrueLit extends BoolLit(true)
case object FalseLit extends BoolLit(false)
