package org.opencypher.spark.impl.prototype

import scala.annotation.tailrec

sealed trait Expr {
  def usedFields: Set[Field] = Set.empty
  def usedLabels: Set[LabelRef] = Set.empty
  def usedRelTypes: Set[RelTypeRef] = Set.empty
  def usedPropertyKeys: Set[PropertyKeyRef] = Set.empty
}

final case class Var(name: String) extends Expr

final case class Connected(source: Field, rel: Field, target: Field) extends Expr {
  override def usedFields = Set(source, rel, target)
}

object Ands {
  def apply(exprs: Expr*): Ands = Ands(exprs.toSet)
  def apply(exprs: Set[Expr]): Ands = new Ands(exprs)
  def unapply(ands: Ands): Option[Set[Expr]] = Some(ands.exprs)
}

final class Ands(_exprs: Set[Expr]) extends Expr with Serializable with Product1[Set[Expr]] {
  val exprs: Set[Expr] =
    if (_exprs.isEmpty) throw new IllegalStateException("Attempt to construct empty Ands") else andExprs(_exprs)

  @tailrec
  private def andExprs(exprs: Set[Expr], result: Set[Expr] = Set.empty): Set[Expr] =
    if (exprs.isEmpty)
      result
    else {
      val expr = exprs.head
      val remaining = exprs.tail
      expr match  {
        case Ands(moreExprs) => andExprs(moreExprs ++ remaining, result)
        case _ => andExprs(remaining, result + expr)
      }
    }

  override def hashCode() = {
    exprs.hashCode() + 31
  }

  override def equals(obj: scala.Any) = obj match {
    case Ands(otherExprs) => exprs == otherExprs
    case _ => false
  }

  override def _1: Set[Expr] = exprs

  override def canEqual(that: Any): Boolean = that match {
    case _: Ands => true
    case _ => false
  }

  override def toString = s"Ands(${exprs.mkString(", ")})"
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
