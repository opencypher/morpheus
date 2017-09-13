/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.expr

import org.opencypher.caps.ir.api.global._
import org.opencypher.caps.api.types._

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

final case class IsNull(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"type(${expr.withoutType}) IS NULL"
}

final case class IsNotNull(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {
  override def withoutType: String = s"type(${expr.withoutType}) IS NOT NULL"
}

// Binary expressions

sealed trait BinaryExpr extends Expr {
  def lhs: Expr

  def rhs: Expr

  def op: String

  override final def toString = s"$lhs $op $rhs"
  override final def withoutType: String = s"${lhs.withoutType} $op ${rhs.withoutType}"
}

final case class Equals(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override val op = "="
}

final case class LessThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override val op = "<"
}

final case class LessThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override val op = "<="
}

final case class GreaterThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override val op = ">"
}

final case class GreaterThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  override val op = ">="
}

final case class In(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard)
  extends BinaryExpr {
  override val op = "IN"
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
  override val op = "+"
}

final case class Subtract(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override val op = "-"
}

final case class Multiply(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override val op = "*"
}

final case class Divide(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {
  override val op = "/"
}

// Functions
sealed trait FunctionExpr extends Expr {
  def expr: Expr

  def name: String

  override final def toString = s"$name($expr)"
  override final def withoutType = s"$name(${expr.withoutType})"

}
final case class Id(expr: Expr)(val cypherType: CypherType = CTWildcard) extends FunctionExpr {
  override val name = "id"
}

final case class Labels(expr: Expr)(val cypherType: CypherType = CTWildcard) extends FunctionExpr {
  override val name = "labels"
}

final case class Type(expr: Expr)(val cypherType: CypherType = CTWildcard) extends FunctionExpr {
  override val name = "type"
}

final case class Exists(expr: Expr)(val cypherType: CypherType = CTWildcard) extends FunctionExpr {
  override val name = "exists"
}

final case class Keys(expr: Expr)(val cypherType: CypherType = CTWildcard) extends FunctionExpr {
  override val name = "keys"
}

// Aggregators
sealed trait Aggregator extends Expr {
  def inner: Option[Expr]
}

final case class Avg(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = Some(expr)
  override def toString = s"avg($expr)"
  override def withoutType: String = s"avg(${expr.withoutType})"
}

final case class CountStar()(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = None
  override def toString = "count(*)"
}

final case class Count(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = Some(expr)
  override def toString = s"count($expr)"
  override def withoutType: String = s"count(${expr.withoutType})"
}

final case class Max(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = Some(expr)
  override def toString = s"max($expr)"
  override def withoutType: String = s"max(${expr.withoutType})"
}

final case class Min(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = Some(expr)
  override def toString = s"min($expr)"
  override def withoutType: String = s"min(${expr.withoutType})"
}

final case class Sum(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {
  override val inner: Option[Expr] = Some(expr)
  override def toString = s"sum($expr)"
  override def withoutType: String = s"sum(${expr.withoutType})"
}

// Literal expressions

sealed trait Lit[T] extends Expr {
  def v: T
  override def withoutType = s"$v"
}

object ListLit {
  def apply(exprs: Expr*): ListLit = new ListLit(exprs.toIndexedSeq)()
}

final case class ListLit(v: IndexedSeq[Expr])(val cypherType: CypherType = CTList(CTVoid)) extends Lit[IndexedSeq[Expr]]

final case class IntegerLit(v: Long)(val cypherType: CypherType = CTInteger) extends Lit[Long]
final case class StringLit(v: String)(val cypherType: CypherType = CTString) extends Lit[String]

sealed abstract class BoolLit(val v: Boolean, val cypherType: CypherType = CTBoolean) extends Lit[Boolean]
final case class TrueLit() extends BoolLit(true)
final case class FalseLit() extends BoolLit(false)
