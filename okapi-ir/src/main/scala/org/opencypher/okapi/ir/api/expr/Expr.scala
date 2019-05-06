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

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr.FlattenOps._
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.impl.BigDecimalSignatures
import org.opencypher.okapi.ir.impl.BigDecimalSignatures.{Addition, Division, Multiplication}
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
  def isElementExpression: Boolean = owner.isDefined
  /**
    * Returns the node/relationship that this expression is owned by, if it is owned.
    * A node/relationship owns its label/key/property mappings
    */
  def owner: Option[Var] = None
  def withOwner(v: Var): Expr = this

  def as(alias: Var) = AliasExpr(this, alias)
  /**
    * When `nullInNullOut` is true, then the expression evaluates to `null`, if any of its inputs evaluate to `null`.
    *
    * Essentially it means that `null` values pass up the evaluation chain from children to parents.
    */
  def nullInNullOut: Boolean = true
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

  override val children: Array[Expr] = Array(expr)

  override def cypherType: CypherType = alias.cypherType

  override def nullInNullOut: Boolean = expr.nullInNullOut

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
  def unapply(arg: Var): Option[String] = Some(arg.name)
  def unnamed: CypherType => Var = apply("")
  def apply(name: String)(cypherType: CypherType = CTAny): Var = cypherType match {
    case n if n.subTypeOf(CTNode.nullable) => NodeVar(name)(n)
    case r if r.subTypeOf(CTRelationship.nullable) => RelationshipVar(name)(r)
    case _ => SimpleVar(name)(cypherType)
  }
}

final case class ListSegment(index: Int, listVar: Var) extends Var {

  override def owner: Option[Var] = Some(listVar)

  override def withOwner(v: Var): ListSegment = copy(listVar = v)

  override def withoutType: String = s"${listVar.withoutType}($index)"

  override def name: String = s"${listVar.name}($index)"

  override def cypherType: CypherType = {
    val innerType = listVar.cypherType match {
      case CTList(inner) => inner

      case CTUnion(options) =>
        options.map {
          case CTList(inner) => inner
          case CTNull => CTNull
          case _ => throw IllegalArgumentException("input cypher type to resolve to a list", listVar.cypherType)
        }.reduceLeft(_ | _)

      case CTNull => CTNull

      case _ => throw IllegalArgumentException("input cypher type to resolve to a list", listVar.cypherType)
    }

    innerType.nullable
  }

}

sealed trait ReturnItem extends Var

final case class NodeVar(name: String)(val cypherType: CypherType = CTNode) extends ReturnItem {

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): NodeVar = expr match {
    case n: NodeVar => n
    case other => other.cypherType match {
      case n if n.subTypeOf(CTNode.nullable) => NodeVar(other.name)(n)
      case o => throw IllegalArgumentException(CTNode, o)
    }
  }
}

final case class RelationshipVar(name: String)(val cypherType: CypherType = CTRelationship) extends ReturnItem {

  override def owner: Option[Var] = Some(this)

  override def withOwner(expr: Var): RelationshipVar = expr match {
    case r: RelationshipVar => r
    case other => other.cypherType match {
      case r if r.subTypeOf(CTRelationship.nullable) => RelationshipVar(other.name)(r)
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
  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
  def apply[E <: Expr](exprs: E*): Expr = exprs.flattenExprs[Ands] match {
    case Nil => TrueLit
    case one :: Nil => one
    case other => Ands(other)
  }
}

final case class Ands(_exprs: List[Expr]) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ands]), "Ands need to be flattened")

  override val cypherType: CypherType = {
    val childTypes = children.map(_.cypherType)
    if (childTypes.contains(CTFalse)) CTFalse
    else CTUnion(childTypes: _*)
  }

  override def nullInNullOut: Boolean = false

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ANDS(${_exprs.map(_.withoutType).mkString(", ")})"

}

object Ors {
  def apply[E <: Expr](exprs: Set[E]): Expr = apply(exprs.toSeq: _*)
  def apply[E <: Expr](exprs: E*): Expr = exprs.flattenExprs[Ors] match {
    case Nil => TrueLit
    case one :: Nil => one
    case other => Ors(other)
  }
}

final case class Ors(_exprs: List[Expr]) extends Expr {
  require(_exprs.forall(!_.isInstanceOf[Ors]), "Ors need to be flattened")

  override val cypherType: CypherType = {
    val childTypes = children.map(_.cypherType)
    if (childTypes.contains(CTTrue)) CTTrue
    else CTUnion(childTypes: _*)
  }

  override def nullInNullOut: Boolean = false

  def exprs: Set[Expr] = _exprs.toSet

  override def withoutType = s"ORS(${_exprs.map(_.withoutType).mkString(", ")})"

}

sealed trait PredicateExpression extends Expr {
  override val cypherType: CypherType = CTBoolean.asNullableAs(inner.cypherType)
  def inner: Expr
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

  override val cypherType: CypherType = CTBoolean
  def inner: Expr = expr
  override def withoutType: String = s"type(${expr.withoutType}) IS NULL"

}

final case class IsNotNull(expr: Expr) extends PredicateExpression {

  override val cypherType: CypherType = CTBoolean
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
  override final def toString = s"$lhs $op $rhs"
  override final def withoutType: String = s"${lhs.withoutType} $op ${rhs.withoutType}"
  def lhs: Expr
  def rhs: Expr
  def op: String
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

  override val cypherType: CypherType = lhs.cypherType -> rhs.cypherType match {
    case (_, CTEmptyList) => CTFalse
    case (CTNull, _) => CTNull
    case (l, _) if l.isNullable => CTBoolean.nullable
    case (_, CTList(inner)) if inner.isNullable => CTBoolean.nullable
    case (l, CTList(inner)) if !l.couldBeSameTypeAs(inner) => CTFalse
    case _ => childNullPropagatesTo(CTBoolean)
  }

}

sealed trait Property extends Expr {

  def propertyOwner: Expr

  def key: PropertyKey

  override def owner: Option[Var] = propertyOwner match {
    case v: Var => Some(v)
    case _ => None
  }

  override def withoutType: String = s"${propertyOwner.withoutType}.${key.name}"

}

final case class ElementProperty(propertyOwner: Expr, key: PropertyKey)(val cypherType: CypherType) extends Property {

  override def withOwner(v: Var): ElementProperty = ElementProperty(v, key)(cypherType)

}

final case class MapProperty(propertyOwner: Expr, key: PropertyKey) extends Property {

  val cypherType: CypherType = propertyOwner.cypherType match {
    case CTMap(inner) =>
      inner.getOrElse(key.name, CTNull)
    case other =>
      throw IllegalArgumentException(s"Map property needs to be defined on a map. `$propertyOwner` is of type `$other`.")
  }

  override def withOwner(v: Var): MapProperty = MapProperty(v, key)

}

final case class DateProperty(propertyOwner: Expr, key: PropertyKey) extends Property {

  val cypherType: CypherType = CTInteger.asNullableAs(propertyOwner.cypherType)

  override def withOwner(v: Var): DateProperty = DateProperty(v, key)

}

final case class LocalDateTimeProperty(propertyOwner: Expr, key: PropertyKey) extends Property {

  val cypherType: CypherType = CTInteger.asNullableAs(propertyOwner.cypherType)

  override def withOwner(v: Var): LocalDateTimeProperty = LocalDateTimeProperty(v, key)

}

final case class DurationProperty(propertyOwner: Expr, key: PropertyKey) extends Property {

  val cypherType: CypherType = CTInteger.asNullableAs(propertyOwner.cypherType)

  override def withOwner(v: Var): DurationProperty = DurationProperty(v, key)

}

final case class MapExpression(items: Map[String, Expr]) extends Expr {

  override def withoutType: String = s"{${items.mapValues(_.withoutType)}}"

  override def cypherType: CypherType = CTMap(items.map { case (key, value) => key -> value.cypherType })
}

// Arithmetic expressions

sealed trait ArithmeticExpr extends BinaryExpr {
  def lhs: Expr

  def rhs: Expr

  override val cypherType: CypherType = {
    val maybeType = signature(lhs.cypherType.material, rhs.cypherType.material)

    maybeType match {
      case Some(typ) => typ.asNullableAs(rhs.cypherType join lhs.cypherType)
      case None => throw UnsupportedOperationException(s"Type signature ${getClass.getSimpleName}(${lhs.cypherType}, ${rhs.cypherType}) is not supported.")
    }
  }

  def signature(lhsType: CypherType, rhsType: CypherType): Option[CypherType]
}

final case class Add(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

  override val op = "+"

  override def signature(lhsType: CypherType, rhsType: CypherType): Option[CypherType] = lhsType -> rhsType match {
    case (CTVoid, _) | (_, CTVoid) => Some(CTNull)
    case (left: CTList, r) => Some(listConcatJoin(left, r))
    case (l, right: CTList) => Some(listConcatJoin(right, l))
    case (CTString, r) if r.subTypeOf(CTNumber) => Some(CTString)
    case (l, CTString) if l.subTypeOf(CTNumber) => Some(CTString)
    case (CTString, CTString) => Some(CTString)
    case (CTDuration, CTDuration) => Some(CTDuration)
    case (CTLocalDateTime, CTDuration) => Some(CTLocalDateTime)
    case (CTDuration, CTLocalDateTime) => Some(CTLocalDateTime)
    case (CTDate, CTDuration) => Some(CTDate)
    case (CTDuration, CTDate) => Some(CTDate)
    case (CTInteger, CTInteger) => Some(CTInteger)
    case (CTFloat, CTInteger) => Some(CTFloat)
    case (CTInteger, CTFloat) => Some(CTFloat)
    case (CTFloat, CTFloat) => Some(CTFloat)
    case (left, right) => BigDecimalSignatures.arithmeticSignature(Addition)(Seq(left, right))
  }


  def listConcatJoin(lhsType: CTList, rhsType: CypherType): CypherType = lhsType -> rhsType match {
    case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
    case (CTList(lInner), _) => CTList(lInner join rhsType)
  }
}

final case class Subtract(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

  override val op = "-"

  override def signature(lhsType: CypherType, rhsType: CypherType): Option[CypherType] = lhsType -> rhsType match {
    case (CTVoid, _) | (_, CTVoid) => Some(CTNull)
    case (CTInteger, CTInteger) => Some(CTInteger)
    case (CTFloat, CTFloat) => Some(CTFloat)
    case (CTInteger, CTFloat) => Some(CTFloat)
    case (CTFloat, CTInteger) => Some(CTFloat)
    case (CTDuration, CTDuration) => Some(CTDuration)
    case (CTLocalDateTime, CTDuration) => Some(CTLocalDateTime)
    case (CTDate, CTDuration) => Some(CTDate)
    case (left, right) => BigDecimalSignatures.arithmeticSignature(Addition)(Seq(left, right))
  }
}

final case class Multiply(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

  override val op = "*"

  override def signature(lhsType: CypherType, rhsType: CypherType): Option[CypherType] = lhsType -> rhsType match {
    case (CTVoid, _) | (_, CTVoid) => Some(CTNull)
    case (CTInteger, CTInteger) => Some(CTInteger)
    case (CTFloat, CTFloat) => Some(CTFloat)
    case (CTInteger, CTFloat) => Some(CTFloat)
    case (CTFloat, CTInteger) => Some(CTFloat)
    case (CTDuration, CTFloat) => Some(CTDuration)
    case (CTDuration, CTInteger) => Some(CTDuration)
    case (CTFloat, CTDuration) => Some(CTDuration)
    case (CTInteger, CTDuration) => Some(CTDuration)
    case (left, right) => BigDecimalSignatures.arithmeticSignature(Multiplication)(Seq(left, right))
  }
}

final case class Divide(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

  override val op = "/"

  override def signature(lhsType: CypherType, rhsType: CypherType): Option[CypherType] = lhsType -> rhsType match {
    case (CTVoid, _) | (_, CTVoid) => Some(CTNull)
    case (CTInteger, CTInteger) => Some(CTInteger)
    case (CTFloat, CTFloat) => Some(CTFloat)
    case (CTInteger, CTFloat) => Some(CTFloat)
    case (CTFloat, CTInteger) => Some(CTFloat)
    case (CTDuration, CTFloat) => Some(CTDuration)
    case (CTDuration, CTInteger) => Some(CTDuration)
    case (left, right) => BigDecimalSignatures.arithmeticSignature(Division)(Seq(left, right))
  }
}

// Functions
sealed trait PropagationType
case object ChildNullPropagation extends PropagationType
case object NullabilityPropagation extends PropagationType
case object FullNullabilityPropagation extends PropagationType


sealed trait FunctionExpr extends Expr {
  override final def toString = s"$name(${exprs.mkString(", ")})"
  override final def withoutType = s"$name(${exprs.map(_.withoutType).mkString(", ")})"
  def name: String = this.getClass.getSimpleName.toLowerCase
  def exprs: List[Expr]
  def propagationType : Option[PropagationType] = None

  def cypherType: CypherType = computeCypherType

  def computeCypherType: CypherType = {
    val materialTypeVector = exprs.map(_.cypherType.material)
    val joinedCypherType = exprs.map(_.cypherType).foldLeft[CypherType](CTVoid)(_ join _)
    val maybeType = signature(materialTypeVector)

    maybeType match {
      case Some(typ) =>
        propagationType match {
         case Some(ChildNullPropagation) => childNullPropagatesTo(typ)
         case Some(NullabilityPropagation) => typ.asNullableAs(joinedCypherType)
         case Some(FullNullabilityPropagation) =>  if (exprs.forall(_.cypherType.isNullable)) typ.nullable else typ
         case None => typ
        }
      case None =>
          if (children.exists(_.cypherType == CTNull)) CTNull //todo: check corner cases (cases where no Propagation is specified)
          else throw UnsupportedOperationException(s"Type signature ${getClass.getSimpleName}($joinedCypherType) is not supported.")
    }
  }

  def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType]
}

sealed trait NullaryFunctionExpr extends FunctionExpr {
  def exprs: List[Expr] = List.empty[Expr]

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = Some(cypherType)
}

final case class MonotonicallyIncreasingId() extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTInteger
}

sealed trait UnaryFunctionExpr extends FunctionExpr {
  def expr: Expr

  def exprs: List[Expr] = List(expr)

  def signature(inputCypherType: CypherType): Option[CypherType]
  def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = signature(inputCypherTypes.head)
}

final case class Id(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTNode(_, _) | CTRelationship(_, _) => Some(CTIdentity)
    case _ => None
  }
}

object PrefixId {
  type GraphIdPrefix = Byte
}

final case class PrefixId(expr: Expr, prefix: GraphIdPrefix) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTIdentity => Some(CTIdentity)
    case _ => None
  }
}

final case class ToId(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
      case CTInteger | CTIdentity => Some(CTIdentity)
      case x if x.subTypeOf(CTElement) => Some(CTIdentity)
      case _ => None
  }
}

final case class Labels(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTNode(_, _) => Some(CTList(CTString))
    case _ => None
  }
}

final case class Type(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTRelationship(_, _) => Some(CTString)
    case _ => None
  }
}

final case class Exists(expr: Expr) extends UnaryFunctionExpr {
  def signature(cypherType: CypherType): Option[CypherType] = Some(CTBoolean)
}

final case class Size(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTList(_) | CTString => Some(CTInteger)
    case _ => None
  }
}

final case class Keys(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTNode(_, _) | CTRelationship(_, _) | CTMap(_) => Some(CTList(CTString))
    case _ => None
  }
}

final case class StartNodeFunction(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTRelationship(_, _) => Some(CTNode)
    case _ => None
  }
}

final case class EndNodeFunction(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTRelationship(_, _) => Some(CTNode)
    case _ => None
  }
}

final case class ToFloat(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
      case CTString => Some(CTFloat)
      case x if x.subTypeOf(CTNumber) => Some(CTFloat)
      case _ => None
    }
}

final case class ToInteger(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTString => Some(CTInteger)
    case x if x.subTypeOf(CTNumber) => Some(CTInteger)
    case _ => None
  }
}

final case class ToString(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTString => Some(CTString)
    case x if x.subTypeOf(CTUnion(CTNumber, CTTemporal, CTBoolean)) => Some(CTString)
    case _ => None
  }
}

final case class ToBoolean(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTString => Some(CTBoolean)
    case x if x.subTypeOf(CTBoolean) => Some(CTBoolean)
    case _ => None
  }
}

object BigDecimal {
  val name: String = "bigdecimal"
}

final case class BigDecimal(expr: Expr, precision: Long, scale: Long) extends UnaryFunctionExpr {
  if (scale > precision) {
    throw IllegalArgumentException("Greater precision than scale", s"precision: $precision, scale: $scale")
  }
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)
  //todo: allowed cyphertypes?
  def signature(cypherType: CypherType): Option[CypherType] = Some(CTBigDecimal(precision.toInt, scale.toInt)) }

final case class Coalesce(exprs: List[Expr]) extends FunctionExpr {
  override def nullInNullOut: Boolean = false

  override def propagationType: Option[PropagationType] = Some(FullNullabilityPropagation)

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = Some(inputCypherTypes.reduceLeft(_ | _))

}

final case class Explode(expr: Expr) extends Expr {

  override def nullInNullOut = false

  override def withoutType: String = s"explode(${expr.withoutType})"

  override def cypherType: CypherType = expr.cypherType match {
    case CTList(inner) => inner

    case CTUnion(options) =>
      options.map {
        case CTList(inner) => inner
        case _ => throw IllegalArgumentException("input cypher type to resolve to a list", expr.cypherType)
      }.reduceLeft(_ | _)

    case CTNull =>
      CTVoid

    case _ => throw IllegalArgumentException("input cypher type to resolve to a list", expr.cypherType)
  }
}

sealed trait UnaryStringFunctionExpr extends UnaryFunctionExpr{
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTString => Some(CTString)
    case _ => None
  }
}

final case class Trim(expr: Expr) extends UnaryStringFunctionExpr

final case class LTrim(expr: Expr) extends UnaryStringFunctionExpr

final case class RTrim(expr: Expr) extends UnaryStringFunctionExpr

final case class ToUpper(expr: Expr) extends UnaryStringFunctionExpr

final case class ToLower(expr: Expr) extends UnaryStringFunctionExpr

final case class Properties(expr: Expr)(override val cypherType: CypherType) extends UnaryFunctionExpr {
  //todo: actually not needed here as type already checked at ExpressionConverter
  override def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case CTNode(_, _) | CTRelationship(_, _) | CTMap(_) => Some(CTMap)
    case _ => None
  }
}

// NAry Function expressions

final case class Range(from: Expr, to: Expr, o: Option[Expr]) extends FunctionExpr {
  override def exprs: List[Expr] = o match {
    case Some(e) => List(from, to, e)
    case None => List(from, to)
  }

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = inputCypherTypes match {
    case Seq(CTInteger, CTInteger) => Some(CTList(CTInteger))
    case Seq(CTInteger, CTInteger, CTInteger) => Some(CTList(CTInteger))
    case _ => None
  }
}

final case class Replace(original: Expr, search: Expr, replacement: Expr) extends FunctionExpr {
  override def exprs: List[Expr] = List(original, search, replacement)

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = inputCypherTypes match {
    case Seq(CTString, CTString, CTString) => Some(CTString)
    case _ => None
  }
}

final case class Substring(original: Expr, start: Expr, length: Option[Expr]) extends FunctionExpr {
  override def exprs: List[Expr] = length match {
    case Some(l) => List(original, start, l)
    case None => List(original, start)
  }

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = inputCypherTypes match {
    case Seq(CTString, CTInteger) => Some(CTString)
    case Seq(CTString, CTInteger, CTInteger) => Some(CTString)
    case _ => None
  }
}

// Bit operators

final case class ShiftLeft(value: Expr, shiftBits: IntegerLit) extends BinaryExpr {
  require(shiftBits.v < 64)

  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = "<<"

  override def cypherType: CypherType = value.cypherType
}

final case class ShiftRightUnsigned(value: Expr, shiftBits: IntegerLit) extends BinaryExpr {
  require(shiftBits.v < 64)

  override def lhs: Expr = value
  override def rhs: Expr = shiftBits
  override def op: String = ">>>"

  override def cypherType: CypherType = value.cypherType
}

final case class BitwiseAnd(lhs: Expr, rhs: Expr) extends BinaryExpr {

  override def op: String = "&"

  override def cypherType: CypherType = lhs.cypherType | rhs.cypherType
}

final case class BitwiseOr(lhs: Expr, rhs: Expr) extends BinaryExpr {

  override def op: String = "|"

  override def cypherType: CypherType = lhs.cypherType | rhs.cypherType
}

// Mathematical functions
sealed abstract class UnaryMathematicalFunctionExpr(outPutCypherType : CypherType) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  def signature(cypherType: CypherType): Option[CypherType] = cypherType match {
    case n if n.subTypeOf(CTNumber) => Some(outPutCypherType)
    case _ => None
  }
}

final case class Sqrt(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Log(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Log10(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Exp(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

case object E extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

case object Pi extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

// Numeric functions

final case class Abs(expr: Expr) extends UnaryMathematicalFunctionExpr(expr.cypherType)

final case class Ceil(expr: Expr) extends UnaryMathematicalFunctionExpr(CTInteger)

final case class Floor(expr: Expr) extends UnaryMathematicalFunctionExpr(CTInteger)

case object Rand extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTFloat
}

final case class Round(expr: Expr) extends UnaryMathematicalFunctionExpr(CTInteger)

final case class Sign(expr: Expr) extends UnaryMathematicalFunctionExpr(CTInteger)

// Trigonometric functions

final case class Acos(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Asin(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Atan(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Atan2(expr1: Expr, expr2: Expr) extends FunctionExpr {
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)
  override def exprs: List[Expr] = List(expr1, expr2)

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = inputCypherTypes match {
    case Seq(c1, c2) if c1.subTypeOf(CTNumber) && c2.subTypeOf(CTNumber) => Some(CTFloat)
    case _ => None
  }
}

final case class Cos(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Cot(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Degrees(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Haversin(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Radians(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Sin(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

final case class Tan(expr: Expr) extends UnaryMathematicalFunctionExpr(CTFloat)

// Time functions

case object Timestamp extends NullaryFunctionExpr {
  override val cypherType: CypherType = CTInteger
}

// Aggregators

sealed trait Aggregator extends Expr {
  def inner: Option[Expr]

  override def nullInNullOut: Boolean = false
}

sealed trait TypedAggregator extends Aggregator {
  def expr: Expr

  override def cypherType: CypherType = {
    val maybeType = signature(expr.cypherType.material)

    maybeType match {
      case Some(typ) => childNullPropagatesTo(typ)
      case None => throw UnsupportedOperationException(s"Type signature ${getClass.getSimpleName}(${expr.cypherType}) is not supported.")
    }
  }

  def signature(exprType: CypherType): Option[CypherType] = {
    exprType match {
      case CTDuration => Some(CTDuration)
      case x if CTNumber.superTypeOf(x) => Some(x)
      case _ => None
    }
  }
}

final case class Avg(expr: Expr) extends TypedAggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"avg($expr)"
  override def withoutType: String = s"avg(${expr.withoutType})"

}

case object CountStar extends Aggregator {

  override val inner: Option[Expr] = None
  override val cypherType: CypherType = CTInteger
  override def withoutType: String = toString
  override def toString = "count(*)"

}

final case class Count(expr: Expr, distinct: Boolean) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)
  override val cypherType: CypherType = CTInteger
  override def toString = s"count($expr)"
  override def withoutType: String = s"count(${expr.withoutType})"

}

final case class Max(expr: Expr) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)
  override val cypherType: CypherType = expr.cypherType
  override def toString = s"max($expr)"
  override def withoutType: String = s"max(${expr.withoutType})"
}

final case class Min(expr: Expr) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)
  override val cypherType: CypherType = expr.cypherType
  override def toString = s"min($expr)"
  override def withoutType: String = s"min(${expr.withoutType})"

}

final case class Sum(expr: Expr) extends TypedAggregator {

  override val inner: Option[Expr] = Some(expr)

  override def toString = s"sum($expr)"
  override def withoutType: String = s"sum(${expr.withoutType})"

}

final case class Collect(expr: Expr, distinct: Boolean) extends Aggregator {

  override val inner: Option[Expr] = Some(expr)
  override val cypherType: CypherType = CTList(expr.cypherType)
  override def toString = s"collect($expr)"
  override def withoutType: String = s"collect(${expr.withoutType})"

}

// Literal expressions

sealed trait Lit[T] extends Expr {
  def v: T

  override def withoutType = s"$v"
}

final case class ListLit(v: List[Expr]) extends Lit[List[Expr]] {
  override def cypherType: CypherType = CTList(v.foldLeft(CTVoid: CypherType)(_ | _.cypherType))
}

sealed abstract class ListSlice(maybeFrom: Option[Expr], maybeTo: Option[Expr]) extends Expr {

  override val cypherType: CypherType = list.cypherType
  def list: Expr
  override def withoutType: String = s"${list.withoutType}[${maybeFrom.map(_.withoutType).getOrElse("")}..${maybeTo.map(_.withoutType).getOrElse("")}]"

}

final case class ListSliceFromTo(list: Expr, from: Expr, to: Expr) extends ListSlice(Some(from), Some(to))

final case class ListSliceFrom(list: Expr, from: Expr) extends ListSlice(Some(from), None)

final case class ListSliceTo(list: Expr, to: Expr) extends ListSlice(None, Some(to))

final case class ContainerIndex(container: Expr, index: Expr)(val cypherType: CypherType) extends Expr {

  override def withoutType: String = s"${container.withoutType}[${index.withoutType}]"

}

final case class IntegerLit(v: Long) extends Lit[Long] {
  override val cypherType: CypherType = CTInteger
}

final case class FloatLit(v: Double) extends Lit[Double] {
  override val cypherType: CypherType = CTFloat
}

final case class StringLit(v: String) extends Lit[String] {
  override val cypherType: CypherType = CTString
}

sealed abstract class TemporalInstant(expr: Option[Expr], outputCypherType: CypherType) extends FunctionExpr {
  override val exprs: List[Expr] = expr.toList
  override def propagationType: Option[PropagationType] = Some(ChildNullPropagation)

  override def signature(inputCypherTypes: Seq[CypherType]): Option[CypherType] = inputCypherTypes.headOption match {
    case Some(CTString)  | Some(CTMap(_)) => Some(outputCypherType)
    case None => Some(outputCypherType)
    case _ => None
  }
}

final case class LocalDateTime(maybeExpr: Option[Expr]) extends TemporalInstant(maybeExpr, CTLocalDateTime)

final case class Date(expr: Option[Expr]) extends TemporalInstant(expr, CTDate)

final case class Duration(expr: Expr) extends UnaryFunctionExpr {
  override def propagationType: Option[PropagationType] = Some(NullabilityPropagation)

  override def signature(cypherType: CypherType): Option[CypherType] = cypherType match{
    case CTMap(_) | CTString => Some(CTDuration)
    case _ => None
  }

}

sealed abstract class BoolLit(val v: Boolean) extends Lit[Boolean]

case object TrueLit extends BoolLit(true) {
  override val cypherType: CypherType = CTTrue
}

case object FalseLit extends BoolLit(false) {
  override val cypherType: CypherType = CTFalse
}

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

final case class CaseExpr(alternatives: List[(Expr, Expr)], default: Option[Expr])
  (val cypherType: CypherType) extends Expr {

  override val children: Array[Expr] = (default ++ alternatives.flatMap { case (cond, value) => Seq(cond, value) }).toArray

  override def withNewChildren(newChildren: Array[Expr]): CaseExpr = {
    val hasDefault = newChildren.length % 2 == 1
    val (newDefault, as) = if (hasDefault) {
      Some(newChildren.head) -> newChildren.tail
    } else {
      None -> newChildren
    }
    val indexed = as.zipWithIndex
    val conditions = indexed.collect { case (c, i) if i % 2 == 0 => c }
    val values = indexed.collect { case (c, i) if i % 2 == 1 => c }
    val newAlternatives = conditions.zip(values).toList
    CaseExpr(newAlternatives, newDefault)(cypherType)
  }

  override def toString: String = s"$withoutType($cypherType)"

  override def withoutType: String = {
    val alternativesString = alternatives
      .map(pair => pair._1.withoutType -> pair._2.withoutType)
      .mkString("[", ", ", "]")
    s"CaseExpr($alternativesString, $default)"
  }

}
