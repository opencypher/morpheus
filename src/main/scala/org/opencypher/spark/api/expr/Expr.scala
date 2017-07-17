package org.opencypher.spark.api.expr

import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.types._

import scala.annotation.tailrec

sealed trait Expr {
  def cypherType: CypherType

  def withoutType: String = toString

  override def toString = s"$withoutType :: $cypherType"
}

final case class Const(constant: Constant)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"$$${constant.name}"
}

final case class Var(name: String)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"$name"
}
final case class StartNode(e: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def toString = s"source($e)"

  override def withoutType: String = s"source(${e.withoutType})"
}
final case class EndNode(e: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def toString = s"target($e)"

  override def withoutType: String = s"target(${e.withoutType})"
}

trait FlatteningOpExprCompanion[T] {
  def apply(exprs: Expr*): T
  def apply(exprs: Set[Expr]): T
  def unapply(expr: Any): Option[Set[Expr]]
}

sealed abstract class FlatteningOpExpr(_exprs: Set[Expr]) extends Expr with Serializable with Product1[Set[Expr]] {

  val exprs: Set[Expr] =
    if (_exprs.isEmpty) throw new IllegalStateException(s"Attempt to construct empty $productPrefix")
    else flatExpr(_exprs)

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
  override def apply(exprs: Expr*): Ands = Ands(exprs.toSet)
  override def apply(exprs: Set[Expr]): Ands = new Ands(exprs)(CTBoolean)
  override def unapply(expr: Any): Option[Set[Expr]] = expr match {
    case ands: Ands => Some(ands.exprs)
    case _ => None
  }
}

final class Ands(_exprs: Set[Expr])(val cypherType: CypherType = CTWildcard) extends FlatteningOpExpr(_exprs) {
  override def productPrefix = "Ands"
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Ands]
  override protected def companion: FlatteningOpExprCompanion[Ands] = Ands
  override protected def hashPrime: Int = 31
}

object Ors extends FlatteningOpExprCompanion[Ors] {
  override def apply(exprs: Expr*): Ors = Ors(exprs.toSet)
  override def apply(exprs: Set[Expr]): Ors = new Ors(exprs)()
  override def unapply(expr: Any): Option[Set[Expr]] = expr match {
    case ors: Ors => Some(ors.exprs)
    case _ => None
  }
}

final class Ors(_exprs: Set[Expr])(val cypherType: CypherType = CTWildcard) extends FlatteningOpExpr(_exprs) {
  override def productPrefix = "Ors"
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Ors]
  override protected def companion: FlatteningOpExprCompanion[Ors] = Ors
  override protected def hashPrime: Int = 61
}

final case class Not(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType = s"NOT ${expr.withoutType}"
}

final case class HasLabel(node: Expr, label: Label)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"${node.withoutType}:${label.name}"
}

final case class HasType(rel: Expr, relType: RelType)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"type(${rel.withoutType}) = '${relType.name}'"
}

// Binary expressions

sealed trait BinaryExpr extends Expr {
  def lhs: Expr

  def rhs: Expr
}

final case class Equals(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override def withoutType: String = s"${lhs.withoutType} = ${rhs.withoutType}"
}

final case class LessThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override def toString = s"$lhs < $rhs"
  override def withoutType: String = s"${lhs.withoutType} < ${rhs.withoutType}"
}

final case class LessThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override def toString: String = s"$lhs <= $rhs"
  override def withoutType: String = s"${lhs.withoutType} <= ${rhs.withoutType}"
}

final case class GreaterThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override def toString: String = s"$lhs > $rhs"
  override def withoutType: String = s"${lhs.withoutType} > ${rhs.withoutType}"
}

final case class GreaterThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override def toString: String = s"$lhs >= $rhs"
  override def withoutType: String = s"${lhs.withoutType} >= ${rhs.withoutType}"
}

final case class Property(m: Expr, key: PropertyKey)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"${m.withoutType}.${key.name}"

  override def equals(obj: scala.Any) = obj match {
    case null => false
    case other: Property => m == other.m && key == other.key && cypherType == other.cypherType
    case _ => false
  }
}
final case class TypeId(rel: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType = s"type(${rel.withoutType})"
}

// Arithmetic expressions

sealed trait ArithmeticExpr extends BinaryExpr {
  def lhs: Expr
  def rhs: Expr
}

final case class Add(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override def toString = s"$lhs + $rhs"
  override def withoutType = s"${lhs.withoutType} + ${rhs.withoutType}"
}

final case class Subtract(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override def toString = s"$lhs - $rhs"
  override def withoutType = s"${lhs.withoutType} - ${rhs.withoutType}"
}

final case class Multiply(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override def toString = s"$lhs * $rhs"
  override def withoutType = s"${lhs.withoutType} * ${rhs.withoutType}"
}

final case class Divide(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override def toString = s"$lhs / $rhs"
  override def withoutType = s"${lhs.withoutType} / ${rhs.withoutType}"
}

// Functions
sealed trait Function extends Expr {
  def expr: Expr
}
final case class Id(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Function {
  override def toString = s"id($expr)"
  override def withoutType = s"id(${expr.withoutType})"
}

// Literal expressions

sealed trait Lit[T] extends Expr {
  def v: T
  override def withoutType = s"$v"
}

final case class IntegerLit(v: Long)(val cypherType: CypherType = CTInteger) extends Lit[Long]
final case class StringLit(v: String)(val cypherType: CypherType = CTString) extends Lit[String]

sealed abstract class BoolLit(val v: Boolean, val cypherType: CypherType = CTBoolean) extends Lit[Boolean]
final case class TrueLit() extends BoolLit(true)
final case class FalseLit() extends BoolLit(false)
