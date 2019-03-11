/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.api.expr

import org.opencypher.okapi.api.types.{CTInteger, _}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr.FlattenOps._
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.trees.AbstractTreeNode

import scala.annotation.tailrec
import scala.reflect.ClassTag

object Expr {

  implicit def alphabeticalOrdering[A <: Expr]: Ordering[Expr] =
    Ordering.by(e => (e.toString, e.toString))
}

/**
  * Describes a Cypher expression.
  *
  * @see [[http://neo4j.com/docs/developer-manual/current/cypher/syntax/expressions/ Cypher Expressions in the Neo4j Manual]]
  */
sealed abstract class Expr extends AbstractTreeNode[Expr] {

  def cypherType: CypherType

  def withoutType: String

  override def toString = s"$withoutType :: $cypherType"

  /**
    * Returns the node/relationship that this expression is owned by, if it is owned.
    * A node/relationship owns its label/key/property mappings
    */
  def owner: Option[Var] = None

  def isEntityExpression: Boolean = owner.isDefined

  def withOwner(v: Var): Expr = this

  def as(alias: Var) = AliasExpr(this, alias)

  protected def childNullPropagatesTo(ct: CypherType): CypherType = {
    if (children.exists(_.cypherType == CTNull)) {
      CTNull
    } else if (children.exists(_.cypherType.isNullable)) {
      ct.nullable
    } else {
      ct
    }
  }

}

final case class AliasExpr(expr: Expr, alias: Var) extends Expr {

  override def cypherType: CypherType = alias.cypherType

  override def withoutType: String = s"$expr AS $alias"

}

final case class Param(name: String)(val cypherType: CypherType) extends Expr {

  override def withoutType: String = s"$$$name"

}

sealed trait Var extends Expr {
  def name: String

  override def withoutType: String = name
}

object Var {
  def apply(name: String)(cypherType: CypherType = CTAny): Var = cypherType match {
    case n: CTNode => NodeVar(name)(n)
    case n: CTNodeOrNull => NodeVar(name)(n)
    case r: CTRelationship => RelationshipVar(name)(r)
    case r: CTRelationshipOrNull => RelationshipVar(name)(r)
    case _ => SimpleVar(name)(cypherType)
  }

  def unapply(arg: Var): Option[String] = Some(arg.name)

  def unnamed: CypherType => Var = apply("")
}

final case class ListSegment(index: Int, listVar: Var)(val cypherType: CypherType) extends Var {

  override def owner: Option[Var] = Some(listVar)

  override def withOwner(v: Var): ListSegment = copy(listVar = v)(cypherType)

  override def withoutType: String = s"${listVar.withoutType}($index)"

  override def name: String = s"${listVar.name}($index)"

}

sealed trait ReturnItem extends Var

final case class NodeVar(name: String)(val cypherType: CypherType = CTNode) extends ReturnItem {

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): NodeVar = expr match {
    case n: NodeVar => n
    case other => other.cypherType match {
      case n: CTNode => NodeVar(other.name)(n)
      case o => throw IllegalArgumentException(CTNode, o)
    }
  }
}

final case class RelationshipVar(name: String)(val cypherType: CypherType = CTRelationship) extends ReturnItem {

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): RelationshipVar = expr match {
    case r: RelationshipVar => r
    case other => other.cypherType match {
      case r: CTRelationship => RelationshipVar(other.name)(r)
      case o => throw IllegalArgumentException(CTRelationship, o)
    }
  }
}

final case class SimpleVar(name: String)(val cypherType: CypherType) extends ReturnItem {

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): SimpleVar = SimpleVar(expr.name)(expr.cypherType)

}

final case class StartNode(rel: Expr)(val cypherType: CypherType) extends Expr {

  type This = StartNode

  override def toString = s"source($rel) :: $cypherType"

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): StartNode = StartNode(v)(cypherType)

  override def withoutType: String = s"source(${rel.withoutType})"

}

final case class EndNode(rel: Expr)(val cypherType: CypherType) extends Expr {

  type This = EndNode

  override def toString = s"target($rel) :: $cypherType"

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): EndNode = EndNode(v)(cypherType)

  override def withoutType: String = s"target(${rel.withoutType})"

}

object FlattenOps {

  // TODO: Implement as a rewriter instead
  implicit class RichExpressions(exprs: Traversable[Expr]) {

    /**
      * Flattens child expressions
      */
    def flattenExprs[E <: Expr : ClassTag]: List[Expr] = {
      @tailrec def flattenRec(es: List[Expr], result: Set[Expr] = Set.empty): Set[Expr] = {
        es match {
          case Nil => result
          case h :: tail =>
            h match {
              case e: E => flattenRec(tail, result ++ e.children.toList)
              case nonE => flattenRec(tail, result + nonE)
            }
        }
      }

      flattenRec(exprs.toList, Set.empty).toList
    }
  }

}

object Ands {

  def apply[E <: Expr](exprs: E*): Expr = exprs.flattenExprs[Ands] match {
    case Nil => TrueLit
    case one :: Nil => one
    case other => Ands(other)
  }

  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
}

final case class Ands(_exprs: List[Expr]) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ands]), "Ands need to be flattened")

  override val cypherType: CypherType = childNullPropagatesTo(CTBoolean)

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ANDS(${_exprs.map(_.withoutType).mkString(", ")})"

}

object Ors {

  def apply[E <: Expr](exprs: E*): Expr = exprs.flattenExprs[Ors] match {
    case Nil => TrueLit
    case one :: Nil => one
    case other => Ors(other)
  }

  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
}

final case class Ors(_exprs: List[Expr]) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ors]), "Ors need to be flattened")

  override val cypherType: CypherType = childNullPropagatesTo(CTBoolean)

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ORS(${_exprs.map(_.withoutType).mkString(", ")})"

}

sealed trait PredicateExpression extends Expr {
  def inner: Expr

  override val cypherType: CypherType = CTBoolean.asNullableAs(inner.cypherType)
}

final case class Not(expr: Expr) extends PredicateExpression {

  def inner: Expr = expr

  override def withoutType = s"NOT(${expr.withoutType})"

}

final case class HasLabel(node: Expr, label: Label) extends PredicateExpression {

  def inner: Expr = node

  override def owner: Option[Var] = node match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): HasLabel = HasLabel(v, label)

  override def withoutType: String = s"${node.withoutType}:${label.name}"

}

final case class HasType(rel: Expr, relType: RelType) extends PredicateExpression {

  def inner: Expr = rel

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): HasType = HasType(v, relType)

  override def withoutType: String = s"${rel.withoutType}:${relType.name}"

}

final case class IsNull(expr: Expr) extends PredicateExpression {

  def inner: Expr = expr

  override def withoutType: String = s"type(${expr.withoutType}) IS NULL"

}

final case class IsNotNull(expr: Expr) extends PredicateExpression {

  def inner: Expr = expr

  override def withoutType: String = s"type(${expr.withoutType}) IS NOT NULL"

}

final case class StartsWith(lhs: Expr, rhs: Expr) extends BinaryPredicate {
  override def op: String = "STARTS WITH"
}

final case class EndsWith(lhs: Expr, rhs: Expr) extends BinaryPredicate {
  override def op: String = "ENDS WITH"
}

final case class Contains(lhs: Expr, rhs: Expr) extends BinaryPredicate {
  override def op: String = "CONTAINS"
}

// Binary expressions

sealed trait BinaryExpr extends Expr {
  def lhs: Expr

  def rhs: Expr

  def op: String

  override final def toString = s"$lhs $op $rhs"

  override final def withoutType: String = s"${lhs.withoutType} $op ${rhs.withoutType}"
}

sealed trait BinaryPredicate extends BinaryExpr {

  override val cypherType: CypherType = childNullPropagatesTo(CTBoolean)

}

final case class Equals(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = "="

}

final case class RegexMatch(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override def op: String = "=~"

}

final case class LessThan(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = "<"

}

final case class LessThanOrEqual(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = "<="

}

final case class GreaterThan(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = ">"

}

final case class GreaterThanOrEqual(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = ">="

}

final case class In(lhs: Expr, rhs: Expr) extends BinaryPredicate {

  override val op = "IN"

  override val cypherType: CypherType = {
    if (rhs.cypherType == CTList(CTVoid)) {
      CTBoolean
    } else {
      childNullPropagatesTo(CTBoolean)
    }
  }

}

final case class Property(entity: Expr, key: PropertyKey)(val cypherType: CypherType) extends Expr {

  override def owner: Option[Var] = entity match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): Property = Property(v, key)(cypherType)

  override def withoutType: String = s"${entity.withoutType}.${key.name}"

}

final case class MapExpression(items: Map[String, Expr])(val cypherType: CypherType) extends Expr {

  override def withoutType: String = s"{${items.mapValues(_.withoutType)}}"

}

// Arithmetic expressions

sealed trait ArithmeticExpr extends BinaryExpr {
  def lhs: Expr

  def rhs: Expr
}

final case class Add(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends ArithmeticExpr {

  override val op = "+"

}

final case class Subtract(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends ArithmeticExpr {

  override val op = "-"

}

final case class Multiply(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends ArithmeticExpr {

  override val op = "*"

}

final case class Divide(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends ArithmeticExpr {

  override val op = "/"

}

// Functions
sealed trait FunctionExpr extends Expr {

  def exprs: IndexedSeq[Expr]

  def name: String = this.getClass.getSimpleName.toLowerCase

  override final def toString = s"$name(${exprs.mkString(", ")})"

  override final def withoutType = s"$name(${exprs.map(_.withoutType).mkString(", ")})"
}

final case class MonotonicallyIncreasingId(cypherType: CypherType = CTInteger) extends FunctionExpr {

  override def exprs: IndexedSeq[Expr] = IndexedSeq.empty

}

sealed trait NullaryFunctionExpr extends FunctionExpr {

  def exprs: IndexedSeq[Expr] = IndexedSeq.empty[Expr]
}

sealed trait UnaryFunctionExpr extends FunctionExpr {

  def expr: Expr

  def exprs: IndexedSeq[Expr] = IndexedSeq(expr)
}

final case class Id(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = childNullPropagatesTo(CTIdentity)
}

object PrefixId {
  type GraphIdPrefix = Byte
}

final case class PrefixId(expr: Expr, prefix: GraphIdPrefix)(val cypherType: CypherType) extends UnaryFunctionExpr

final case class ToId(expr: Expr)(val cypherType: CypherType = CTIdentity) extends UnaryFunctionExpr

final case class Labels(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = childNullPropagatesTo(CTList(CTString))
}

final case class Type(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = childNullPropagatesTo(CTString)
}

final case class Exists(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTBoolean
}

final case class Size(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = childNullPropagatesTo(CTInteger)
}

final case class Keys(expr: Expr)(val cypherType: CypherType) extends UnaryFunctionExpr

final case class StartNodeFunction(expr: Expr)(val cypherType: CypherType) extends UnaryFunctionExpr

final case class EndNodeFunction(expr: Expr)(val cypherType: CypherType) extends UnaryFunctionExpr

final case class ToFloat(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class ToInteger(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTInteger.asNullableAs(expr.cypherType)
}

final case class ToString(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class ToBoolean(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTBoolean.asNullableAs(expr.cypherType)
}

final case class Coalesce(exprs: IndexedSeq[Expr])(val cypherType: CypherType) extends FunctionExpr

final case class Explode(expr: Expr)(val cypherType: CypherType) extends Expr {

  override def withoutType: String = s"explode(${expr.withoutType})"

}

final case class Trim(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class LTrim(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class RTrim(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class ToUpper(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class ToLower(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTString.asNullableAs(expr.cypherType)
}

final case class Properties(expr: Expr)(val cypherType: CypherType) extends UnaryFunctionExpr

// NAry Function expressions

final case class Range(from: Expr, to: Expr, o: Option[Expr]) extends FunctionExpr {

  override def exprs: IndexedSeq[Expr] = IndexedSeq(from, to)
  override def cypherType: CypherType = CTList(CTInteger)
}

final case class Replace(original: Expr, search: Expr, replacement: Expr) extends FunctionExpr {
  override def exprs: IndexedSeq[Expr] = IndexedSeq(original, search, replacement)
  override def cypherType: CypherType = CTString
}

final case class Substring(original: Expr, start: Expr, length: Option[Expr]) extends FunctionExpr {
  override def exprs: IndexedSeq[Expr] = IndexedSeq(original, start)
  override def cypherType: CypherType = CTString
}

// Bit operators

final case class ShiftLeft(value: Expr, shiftBits: IntegerLit)(val cypherType: CypherType) extends BinaryExpr {
  require(shiftBits.v < 64)

  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = "<<"
}

final case class ShiftRightUnsigned(value: Expr, shiftBits: IntegerLit)(val cypherType: CypherType) extends BinaryExpr {
  require(shiftBits.v < 64)

  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = ">>>"
}

final case class BitwiseAnd(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends BinaryExpr {

  override def op: String = "&"
}

final case class BitwiseOr(lhs: Expr, rhs: Expr)(val cypherType: CypherType) extends BinaryExpr {

  override def op: String = "|"
}

// Mathematical functions

final case class Sqrt(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Log(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Log10(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Exp(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

case object E extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

case object Pi extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

// Numeric functions

final case class Abs(expr: Expr)(val cypherType: CypherType) extends UnaryFunctionExpr

final case class Ceil(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTInteger.asNullableAs(expr.cypherType)
}

final case class Floor(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTInteger.asNullableAs(expr.cypherType)
}

case object Rand extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

final case class Round(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTInteger.asNullableAs(expr.cypherType)
}

final case class Sign(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTInteger.asNullableAs(expr.cypherType)
}

// Trigonometric functions

final case class Acos(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Asin(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Atan(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Atan2(expr1: Expr, expr2: Expr) extends FunctionExpr {
  override def exprs: IndexedSeq[Expr] = IndexedSeq(expr1, expr2)

  override val cypherType: CypherType = childNullPropagatesTo(CTFloat)
}

final case class Cos(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Cot(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Degrees(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Haversin(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Radians(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Sin(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

final case class Tan(expr: Expr) extends UnaryFunctionExpr {
  override val cypherType: CypherType = CTFloat.asNullableAs(expr.cypherType)
}

// Time functions

case object Timestamp extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTInteger
}

// Aggregators

sealed trait Aggregator extends Expr {
  def inner: Option[Expr]
}

final case class Avg(expr: Expr) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"avg($expr)"

  override def withoutType: String = s"avg(${expr.withoutType})"

  override val cypherType: CypherType = childNullPropagatesTo(CTFloat)

}

case object CountStar extends Aggregator {

  override val inner: Option[Expr] = None

  override def toString = "count(*)"

  override def withoutType: String = toString

  override val cypherType: CypherType = CTInteger

}

final case class Count(expr: Expr, distinct: Boolean) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"count($expr)"

  override def withoutType: String = s"count(${expr.withoutType})"

  override val cypherType: CypherType = CTInteger

}

final case class Max(expr: Expr)(val cypherType: CypherType) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"max($expr)"

  override def withoutType: String = s"max(${expr.withoutType})"

}

final case class Min(expr: Expr)(val cypherType: CypherType) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"min($expr)"

  override def withoutType: String = s"min(${expr.withoutType})"

}

final case class Sum(expr: Expr)(val cypherType: CypherType) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"sum($expr)"

  override def withoutType: String = s"sum(${expr.withoutType})"

}

final case class Collect(expr: Expr, distinct: Boolean) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"collect($expr)"

  override def withoutType: String = s"collect(${expr.withoutType})"

  override val cypherType: CypherType = CTList(expr.cypherType)

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

final case class ContainerIndex(container: Expr, index: Expr)(val cypherType: CypherType) extends Expr {

  override def withoutType: String = s"${container.withoutType}[${index.withoutType}]"

}

final case class IntegerLit(v: Long) extends Lit[Long] {
  override val cypherType: CypherType = CTInteger
}

final case class StringLit(v: String) extends Lit[String] {
  override val cypherType: CypherType = CTString
}

sealed abstract class TemporalInstant(expr: Option[Expr]) extends FunctionExpr {

  override val exprs: IndexedSeq[Expr] = expr match {
    case Some(expr) => IndexedSeq(expr)
    case None => IndexedSeq.empty
  }
}

final case class LocalDateTime(maybeExpr: Option[Expr]) extends TemporalInstant(maybeExpr) {

  override val cypherType: CypherType = childNullPropagatesTo(CTLocalDateTime)

}

final case class Date(expr: Option[Expr]) extends TemporalInstant(expr) {

  override val cypherType: CypherType = childNullPropagatesTo(CTDate)

}

final case class Duration(expr: Expr) extends FunctionExpr {

  override val cypherType: CypherType = CTDuration.asNullableAs(expr.cypherType)

  override def exprs: IndexedSeq[Expr] = IndexedSeq(expr)
}

sealed abstract class BoolLit(val v: Boolean) extends Lit[Boolean] {
  override val cypherType: CypherType = CTBoolean
}

case object TrueLit extends BoolLit(true)

case object FalseLit extends BoolLit(false)

case object NullLit extends Lit[Null] {

  override val cypherType: CypherType = CTNull

  override def v: Null = null
}

// Pattern Predicate Expression

final case class ExistsPatternExpr(targetField: Var, ir: CypherQuery)
  extends Expr {

  override val cypherType: CypherType = CTBoolean

  override def toString = s"$withoutType($cypherType)"

  override def withoutType = s"Exists($targetField)"

}

final case class CaseExpr(alternatives: IndexedSeq[(Expr, Expr)], default: Option[Expr])
  (val cypherType: CypherType) extends Expr {

  override def toString: String = s"$withoutType($cypherType)"

  override def withoutType: String = {
    val alternativesString = alternatives
      .map(pair => pair._1.withoutType -> pair._2.withoutType)
      .mkString("[", ", ", "]")
    s"CaseExpr($alternativesString, $default)"
  }

}
