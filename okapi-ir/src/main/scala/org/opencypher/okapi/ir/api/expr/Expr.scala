/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr.FlattenOps._
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

  type This >: this.type <: Expr

  def cypherType: CypherType

  def withCypherType(ct: CypherType): This

  def withoutType: String

  override def toString = s"$withoutType :: $cypherType"

  /**
    * Returns the node/relationship that this expression is owned by, if it is owned.
    * A node/relationship owns its label/key/property mappings
    */
  def owner: Option[Var] = None

  def isEntityExpression: Boolean = owner.isDefined

  def withOwner(v: Var): This = this

  def as(alias: Var) = AliasExpr(this, alias)
}

final case class AliasExpr(expr: Expr, alias: Var) extends Expr {

  override type This = AliasExpr

  override def cypherType: CypherType = expr.cypherType

  override def withoutType: String = s"$expr AS $alias"

  override def withCypherType(ct: CypherType): AliasExpr = copy(expr.withCypherType(ct), alias)
}

final case class Param(name: String)(val cypherType: CypherType = CTWildcard) extends Expr {

  override type This = Param

  override def withoutType: String = s"$$$name"

  override def withCypherType(ct: CypherType): Param = copy()(ct)
}

sealed trait Var extends Expr {
  def name: String

  override def withoutType: String = s"`$name`"
}

object Var {
  def apply(name: String)(cypherType: CypherType = CTWildcard): Var = cypherType match {
    case n: CTNode => NodeVar(name)(n)
    case n: CTNodeOrNull => NodeVar(name)(n)
    case r: CTRelationship => RelationshipVar(name)(r)
    case r: CTRelationshipOrNull => RelationshipVar(name)(r)
    case _ => SimpleVar(name)(cypherType)
  }

  def unapply(arg: Var): Option[String] = Some(arg.name)
}

final case class ListSegment(index: Int, listVar: Var)(val cypherType: CypherType = CTWildcard) extends Var {
  override type This = ListSegment

  override def owner: Option[Var] = Some(listVar)

  override def withOwner(v: Var): ListSegment = copy(listVar = v)(cypherType)

  override def withoutType: String = s"${listVar.withoutType}($index)"

  override def name: String = s"${listVar.name}($index)"

  override def withCypherType(ct: CypherType): ListSegment = copy()(ct)
}

sealed trait ReturnItem extends Var

final case class NodeVar(name: String)(val cypherType: CypherType = CTNode) extends ReturnItem {

  override type This = NodeVar

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): NodeVar = expr match {
    case n: NodeVar => n
    case other => other.cypherType match {
      case n: CTNode => NodeVar(other.name)(n)
      case o => throw IllegalArgumentException(CTNode, o)
    }
  }

  override def withoutType: String = s"$name"

  override def withCypherType(ct: CypherType): NodeVar = ct.material match {
    case _: CTNode => copy()(ct)
    case other => throw IllegalArgumentException("CTNode type", other)
  }
}

final case class RelationshipVar(name: String)(val cypherType: CypherType = CTRelationship) extends ReturnItem {

  override type This = RelationshipVar

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): RelationshipVar = expr match {
    case r: RelationshipVar => r
    case other => other.cypherType match {
      case r: CTRelationship => RelationshipVar(other.name)(r)
      case o => throw IllegalArgumentException(CTRelationship, o)
    }
  }
  override def withoutType: String = s"$name"

  override def withCypherType(ct: CypherType): RelationshipVar = ct.material match {
    case _: CTRelationship => copy()(ct)
    case other => throw IllegalArgumentException("CTRelationship type", other)
  }
}

final case class SimpleVar(name: String)(val cypherType: CypherType) extends ReturnItem {

  override type This = SimpleVar

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): SimpleVar = SimpleVar(expr.name)(expr.cypherType)

  override def withoutType: String = s"$name"

  override def withCypherType(ct: CypherType): SimpleVar = copy()(ct)
}

final case class StartNode(rel: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {

  type This = StartNode

  override def toString = s"source($rel) :: $cypherType"

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): StartNode = StartNode(v)(cypherType)

  override def withoutType: String = s"source(${rel.withoutType})"

  override def withCypherType(ct: CypherType): StartNode = copy()(ct)
}

final case class EndNode(rel: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {

  type This = EndNode

  override def toString = s"target($rel) :: $cypherType"

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): EndNode = EndNode(v)(cypherType)

  override def withoutType: String = s"target(${rel.withoutType})"

  override def withCypherType(ct: CypherType): EndNode = copy()(ct)
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
    case other => Ands(other)(CTBoolean)
  }

  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
}

final case class Ands(_exprs: List[Expr])(val cypherType: CypherType = CTBoolean) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ands]), "Ands need to be flattened")

  override type This = Ands

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ANDS(${_exprs.map(_.withoutType).mkString(", ")})"

  override def withCypherType(ct: CypherType): Ands = ct.material match {
    case CTBoolean => copy()(ct)
    case other => throw IllegalArgumentException("CTBoolean type", other)
  }
}

object Ors {

  def apply[E <: Expr](exprs: E*): Expr = exprs.flattenExprs[Ors] match {
    case Nil => TrueLit
    case one :: Nil => one
    case other => Ors(other)(CTBoolean)
  }

  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
}

final case class Ors(_exprs: List[Expr])(val cypherType: CypherType = CTBoolean) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ors]), "Ors need to be flattened")

  override type This = Ors

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ORS(${_exprs.map(_.withoutType).mkString(", ")})"

  override def withCypherType(ct: CypherType): Ors = ct.material match {
    case CTBoolean => copy()(ct)
    case other => throw IllegalArgumentException("CTBoolean type", other)
  }
}

sealed trait PredicateExpression extends Expr {
  def inner: Expr
}

final case class Not(expr: Expr)(val cypherType: CypherType = CTWildcard) extends PredicateExpression {

  override type This = Not

  def inner: Expr = expr

  override def withoutType = s"NOT ${expr.withoutType}"

  override def withCypherType(ct: CypherType): Not = copy()(ct)
}

final case class HasLabel(node: Expr, label: Label)
  (val cypherType: CypherType = CTWildcard) extends PredicateExpression {

  override type This = HasLabel

  def inner: Expr = node

  override def owner: Option[Var] = node match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): HasLabel = HasLabel(v, label)(cypherType)

  override def withoutType: String = s"${node.withoutType}:${label.name}"

  override def withCypherType(ct: CypherType): HasLabel = copy()(ct)
}

final case class HasType(rel: Expr, relType: RelType)
  (val cypherType: CypherType = CTWildcard) extends PredicateExpression {

  override type This = HasType

  def inner: Expr = rel

  override def owner: Option[Var] = rel match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): HasType = HasType(v, relType)(cypherType)

  override def withoutType: String = s"type(${rel.withoutType}) = '${relType.name}'"

  override def withCypherType(ct: CypherType): HasType = copy()(ct)
}

final case class IsNull(expr: Expr)(val cypherType: CypherType = CTWildcard) extends PredicateExpression {

  override type This = IsNull

  def inner: Expr = expr

  override def withoutType: String = s"type(${expr.withoutType}) IS NULL"

  override def withCypherType(ct: CypherType): IsNull = copy()(ct)
}

final case class IsNotNull(expr: Expr)(val cypherType: CypherType = CTWildcard) extends PredicateExpression {

  override type This = IsNotNull

  def inner: Expr = expr

  override def withoutType: String = s"type(${expr.withoutType}) IS NOT NULL"

  override def withCypherType(ct: CypherType): IsNotNull = copy()(ct)
}

final case class StartsWith(lhs: Expr, rhs: Expr) extends BinaryExpr {
  override def op: String = "StartsWith"
  override type This = StartsWith
  override def cypherType: CypherType = if((lhs.cypherType join rhs.cypherType).isNullable) CTBoolean.nullable else CTBoolean
  override def withCypherType(ct: CypherType): StartsWith.this.type = this
}

final case class EndsWith(lhs: Expr, rhs: Expr) extends BinaryExpr {
  override def op: String = "EndsWith"
  override type This = EndsWith
  override def cypherType: CypherType = if((lhs.cypherType join rhs.cypherType).isNullable) CTBoolean.nullable else CTBoolean
  override def withCypherType(ct: CypherType): EndsWith = this
}

final case class Contains(lhs: Expr, rhs: Expr) extends BinaryExpr {
  override def op: String = "Contains"
  override type This = Contains
  override def cypherType: CypherType = if((lhs.cypherType join rhs.cypherType).isNullable) CTBoolean.nullable else CTBoolean
  override def withCypherType(ct: CypherType): Contains = this
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

  override type This = Equals

  override val op = "="

  override def withCypherType(ct: CypherType): Equals = copy()(ct)
}

final case class RegexMatch(lhs: Expr, rhs: Expr) extends BinaryExpr {
  override def op: String = "=~"
  override type This = RegexMatch
  override def cypherType: CypherType = if((lhs.cypherType join rhs.cypherType).isNullable) CTBoolean.nullable else CTBoolean
  override def withCypherType(ct: CypherType): RegexMatch = this
}

final case class LessThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = LessThan

  override val op = "<"

  override def withCypherType(ct: CypherType): LessThan = copy()(ct)
}

final case class LessThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = LessThanOrEqual

  override val op = "<="

  override def withCypherType(ct: CypherType): LessThanOrEqual = copy()(ct)
}

final case class GreaterThan(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = GreaterThan

  override val op = ">"

  override def withCypherType(ct: CypherType): GreaterThan = copy()(ct)
}

final case class GreaterThanOrEqual(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = GreaterThanOrEqual

  override val op = ">="

  override def withCypherType(ct: CypherType): GreaterThanOrEqual = copy()(ct)
}

final case class In(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = In

  override val op = "IN"

  override def withCypherType(ct: CypherType): In = copy()(ct)
}

final case class Property(entity: Expr, key: PropertyKey)(val cypherType: CypherType = CTWildcard) extends Expr {

  override type This = Property

  override def owner: Option[Var] = entity match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withOwner(v: Var): Property = Property(v, key)(cypherType)

  override def withoutType: String = s"${entity.withoutType}.${key.name}"

  override def withCypherType(ct: CypherType): Property = copy()(ct)
}

final case class MapExpression(items: Map[String, Expr])(val cypherType: CypherType = CTWildcard) extends Expr {

  override type This = MapExpression

  override def withoutType: String = s"{${items.mapValues(_.withoutType)}}"

  override def withCypherType(ct: CypherType): MapExpression = copy()(ct)
}

// Arithmetic expressions

sealed trait ArithmeticExpr extends BinaryExpr {
  def lhs: Expr

  def rhs: Expr
}

final case class Add(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {

  override type This = Add

  override val op = "+"

  override def withCypherType(ct: CypherType): Add = copy()(ct)
}

final case class Subtract(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {

  override type This = Subtract

  override val op = "-"

  override def withCypherType(ct: CypherType): Subtract = copy()(ct)
}

final case class Multiply(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {

  override type This = Multiply

  override val op = "*"

  override def withCypherType(ct: CypherType): Multiply = copy()(ct)
}

final case class Divide(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends ArithmeticExpr {

  override type This = Divide

  override val op = "/"

  override def withCypherType(ct: CypherType): Divide = copy()(ct)
}

// Functions
sealed trait FunctionExpr extends Expr {

  def exprs: IndexedSeq[Expr]

  def name: String = this.getClass.getSimpleName.toLowerCase

  override final def toString = s"$name(${exprs.mkString(", ")})"

  override final def withoutType = s"$name(${exprs.map(_.withoutType).mkString(", ")})"
}

final case class MonotonicallyIncreasingId(cypherType: CypherType = CTInteger) extends FunctionExpr {

  override type This = MonotonicallyIncreasingId

  override def exprs: IndexedSeq[Expr] = IndexedSeq.empty

  override def withCypherType(ct: CypherType): MonotonicallyIncreasingId = copy(ct)
}

sealed trait NullaryFunctionExpr extends FunctionExpr {

  def exprs: IndexedSeq[Expr] = IndexedSeq.empty[Expr]
}

sealed trait UnaryFunctionExpr extends FunctionExpr {

  def expr: Expr

  def exprs: IndexedSeq[Expr] = IndexedSeq(expr)
}

final case class Id(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Id

  override def withCypherType(ct: CypherType): Id = copy()(ct)
}

final case class Labels(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Labels

  override def withCypherType(ct: CypherType): Labels = copy()(ct)
}

final case class Type(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Type

  override def withCypherType(ct: CypherType): Type = copy()(ct)
}

final case class Exists(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Exists

  override def withCypherType(ct: CypherType): Exists = copy()(ct)
}

final case class Size(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Size

  override def withCypherType(ct: CypherType): Size = copy()(ct)
}

final case class Keys(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Keys

  override def withCypherType(ct: CypherType): Keys = copy()(ct)
}

final case class StartNodeFunction(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = StartNodeFunction

  override def withCypherType(ct: CypherType): StartNodeFunction = copy()(ct)
}

final case class EndNodeFunction(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = EndNodeFunction

  override def withCypherType(ct: CypherType): EndNodeFunction = copy()(ct)
}

final case class ToFloat(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = ToFloat

  override def withCypherType(ct: CypherType): ToFloat = copy()(ct)
}

final case class ToInteger(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = ToInteger

  override def withCypherType(ct: CypherType): ToInteger = copy()(ct)
}

final case class ToString(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = ToString

  override def withCypherType(ct: CypherType): ToString = copy()(ct)
}

final case class ToBoolean(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = ToBoolean

  override def withCypherType(ct: CypherType): ToBoolean = copy()(ct)
}

final case class Coalesce(exprs: IndexedSeq[Expr])(val cypherType: CypherType = CTWildcard) extends FunctionExpr {

  override type This = Coalesce

  override def withCypherType(ct: CypherType): Coalesce = copy()(ct)
}

final case class Explode(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {

  override type This = Explode

  override def withCypherType(ct: CypherType): Explode = copy()(ct)
}

// NAry Function expressions

final case class Range(from: Expr, to: Expr, o: Option[Expr]) extends FunctionExpr {

  override def exprs: IndexedSeq[Expr] = IndexedSeq(from, to)
  override def cypherType: CypherType = CTList(CTInteger)
  override type This = Range

  override def withCypherType(ct: CypherType): Range = this
}

final case class Substring(original: Expr, start: Expr, length: Option[Expr]) extends FunctionExpr {

  override def exprs: IndexedSeq[Expr] = IndexedSeq(original, start)
  override def cypherType: CypherType = CTString
  override type This = Substring

  override def withCypherType(ct: CypherType): Substring = this
}

// Bit operators

final case class ShiftLeft(value: Expr, shiftBits: IntegerLit)
  (val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  require(shiftBits.v < 64)

  override type This = ShiftLeft
  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = "<<"
  override def withCypherType(ct: CypherType): ShiftLeft = copy()(ct)
}

final case class ShiftRightUnsigned(value: Expr, shiftBits: IntegerLit)
  (val cypherType: CypherType = CTWildcard) extends BinaryExpr {
  require(shiftBits.v < 64)

  override type This = ShiftRightUnsigned
  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = ">>>"
  override def withCypherType(ct: CypherType): ShiftRightUnsigned = copy()(ct)
}

final case class BitwiseAnd(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = BitwiseAnd
  override def op: String = "&"
  override def withCypherType(ct: CypherType): BitwiseAnd = copy()(ct)
}

final case class BitwiseOr(lhs: Expr, rhs: Expr)(val cypherType: CypherType = CTWildcard) extends BinaryExpr {

  override type This = BitwiseOr
  override def op: String = "|"
  override def withCypherType(ct: CypherType): BitwiseOr = copy()(ct)
}

// Mathematical functions

final case class Sqrt(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Sqrt
  override def withCypherType(ct: CypherType): Sqrt = copy()(ct)
}

final case class Log(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Log
  override def withCypherType(ct: CypherType): Log = copy()(ct)
}

final case class Log10(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Log10
  override def withCypherType(ct: CypherType): Log10 = copy()(ct)
}

final case class Exp(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Exp
  override def withCypherType(ct: CypherType): Exp = copy()(ct)
}

final case class E()(val cypherType: CypherType = CTWildcard) extends NullaryFunctionExpr {
  override type This = E
  override def withCypherType(ct: CypherType): E = copy()(ct)
}

final case class Pi()(val cypherType: CypherType = CTWildcard) extends NullaryFunctionExpr {
  override type This = Pi
  override def withCypherType(ct: CypherType): Pi = copy()(ct)
}

// Numeric functions

final case class Abs(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Abs
  override def withCypherType(ct: CypherType): Abs = copy()(ct)
}

final case class Ceil(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Ceil
  override def withCypherType(ct: CypherType): Ceil = copy()(ct)
}

final case class Floor(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Floor
  override def withCypherType(ct: CypherType): Floor = copy()(ct)
}

final case class Rand()(val cypherType: CypherType = CTWildcard) extends NullaryFunctionExpr {
  override type This = Rand
  override def withCypherType(ct: CypherType): Rand = copy()(ct)
}

final case class Round(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Round
  override def withCypherType(ct: CypherType): Round = copy()(ct)
}

final case class Sign(expr: Expr)(val cypherType: CypherType = CTWildcard) extends UnaryFunctionExpr {
  override type This = Sign
  override def withCypherType(ct: CypherType): Sign = copy()(ct)
}

// Aggregators

sealed trait Aggregator extends Expr {
  def inner: Option[Expr]
}

final case class Avg(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Avg

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"avg($expr)"

  override def withoutType: String = s"avg(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Avg = copy()(ct)
}

final case class CountStar(cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = CountStar

  override val inner: Option[Expr] = None

  override def toString = "count(*)"

  override def withoutType: String = toString

  override def withCypherType(ct: CypherType): CountStar = copy(ct)
}

final case class Count(expr: Expr, distinct: Boolean)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Count

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"count($expr)"

  override def withoutType: String = s"count(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Count = copy()(ct)
}

final case class Max(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Max

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"max($expr)"

  override def withoutType: String = s"max(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Max = copy()(ct)
}

final case class Min(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Min

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"min($expr)"

  override def withoutType: String = s"min(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Min = copy()(ct)
}

final case class Sum(expr: Expr)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Sum

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"sum($expr)"

  override def withoutType: String = s"sum(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Sum = copy()(ct)
}

final case class Collect(expr: Expr, distinct: Boolean)(val cypherType: CypherType = CTWildcard) extends Aggregator {

  override type This = Collect

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"collect($expr)"

  override def withoutType: String = s"collect(${expr.withoutType})"

  override def withCypherType(ct: CypherType): Collect = copy()(ct)
}

// Literal expressions

sealed trait Lit[T] extends Expr {
  def v: T

  override def withoutType = s"$v"
}

object ListLit {
  def apply(exprs: Expr*): ListLit = new ListLit(exprs.toIndexedSeq)()
}

final case class ListLit(v: IndexedSeq[Expr])
  (val cypherType: CypherType = CTList(CTVoid)) extends Lit[IndexedSeq[Expr]] {

  override type This = ListLit

  override def withCypherType(ct: CypherType): ListLit = copy()(ct)
}

final case class ContainerIndex(container: Expr, index: Expr)(val cypherType: CypherType = CTWildcard) extends Expr {

  override type This = ContainerIndex

  override def withoutType: String = s"${container.withoutType}[${index.withoutType}]"

  override def withCypherType(ct: CypherType): ContainerIndex = copy()(ct)
}

final case class IntegerLit(v: Long)(val cypherType: CypherType = CTInteger) extends Lit[Long] {

  override type This = IntegerLit

  override def withCypherType(ct: CypherType): IntegerLit = copy()(ct)
}

final case class StringLit(v: String)(val cypherType: CypherType = CTString) extends Lit[String] {

  override type This = StringLit

  override def withCypherType(ct: CypherType): StringLit = copy()(ct)
}

sealed abstract class BoolLit(val v: Boolean)(val cypherType: CypherType = CTBoolean) extends Lit[Boolean] {
  override type This = BoolLit
}

case object TrueLit extends BoolLit(true)() {

  override def withCypherType(ct: CypherType): BoolLit = ct match {
    case CTBoolean => TrueLit
    case other => throw IllegalArgumentException("CTBoolean type", other)
  }
}

case object FalseLit extends BoolLit(false)() {

  override def withCypherType(ct: CypherType): BoolLit = ct match {
    case CTBoolean => FalseLit
    case other => throw IllegalArgumentException("CTBoolean type", other)
  }
}

case class NullLit(cypherType: CypherType = CTNull) extends Lit[Null] {

  override type This = NullLit

  override def v: Null = null

  override def withCypherType(ct: CypherType): NullLit =
    if (ct.isNullable) copy(ct) else throw IllegalArgumentException("Nullable type", ct)
}

// Pattern Predicate Expression

final case class ExistsPatternExpr(targetField: Var, ir: CypherQuery)(val cypherType: CypherType = CTBoolean)
  extends Expr {

  override type This = ExistsPatternExpr

  override def toString = s"$withoutType($cypherType)"

  override def withoutType = s"Exists(${ir.info.singleLine}, $targetField)"

  override def withCypherType(ct: CypherType): ExistsPatternExpr = copy()(ct)
}

final case class CaseExpr(alternatives: IndexedSeq[(Expr, Expr)], default: Option[Expr])
  (val cypherType: CypherType = CTWildcard) extends Expr {

  override type This = CaseExpr

  override def toString: String = s"$withoutType($cypherType)"

  override def withoutType: String = {
    val alternativesString = alternatives
      .map(pair => pair._1.withoutType -> pair._2.withoutType)
      .mkString("[", ", ", "]")
    s"CaseExpr($alternativesString, $default)"
  }

  override def withCypherType(ct: CypherType): CaseExpr = copy()(ct)
}
