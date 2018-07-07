package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.types.CypherType.{AnyNode, AnyRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._

import scala.language.postfixOps

object CypherType {

  val CTNumber: CypherType = CTInteger union CTFloat

  val AnyNode: CTNode = CTNode()

  val AnyRelationship: CTRelationship = CTRelationship()

  def parse(typeString: String): CypherType = {
    ???
  }

  implicit class TypeCypherValue(cv: CypherValue) {
    def cypherType: CypherType = {
      cv match {
        case CypherNull => CTNull
        case CypherBoolean(_) => CTBoolean
        case CypherFloat(_) => CTFloat
        case CypherInteger(_) => CTInteger
        case CypherString(_) => CTString
        case CypherNode(_, labels, _) => CTNode(labels)
        case CypherRelationship(_, _, _, relType, _) => CTRelationship(relType)
        case CypherList(l) => CTList(l.map(_.cypherType).foldLeft[CypherType](CTVoid)(_ union _))
      }
    }
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x union y
  }

  implicit def rw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => parse(s))

}

trait CypherType {

  def alternatives: Set[BasicType]

  def material: Set[BasicType] = alternatives.filterNot(_ == CTNull)

  def subTypeOf(other: CypherType): Boolean = {
    other == CTAny || this == other || alternatives.subsetOf(other.alternatives)
  }

  def superTypeOf(other: CypherType): Boolean = {
    def entityComparison: Boolean = () || (isRelationship && other.isRelationship)
    this == CTAny ||
      this == other ||
      (isNode && alternatives.contains(AnyNode)) ||
      (isRelationship && alternatives.contains(AnyRelationship)) ||
      other.alternatives.subsetOf(alternatives)
  }

  private def expandAnyRelationship(lhs: Set[BasicType], rhs: Set[BasicType]): (Set[BasicType], Set[BasicType]) = {
    val lhsWithAnyRel = if (lhs.contains(AnyRelationship)) (lhs - AnyRelationship) ++ rhs.filter(_.isRelationship) else lhs
    val rhsWithAnyRel = if (rhs.contains(AnyRelationship)) (rhs - AnyRelationship) ++ lhs.filter(_.isRelationship) else rhs
    lhsWithAnyRel -> rhsWithAnyRel
  }

  private def expandAnyNode(lhs: Set[BasicType], rhs: Set[BasicType]): (Set[BasicType], Set[BasicType]) = {
    val lhsWithAnyNode = if (lhs.contains(AnyNode)) (lhs - AnyNode) ++ rhs.filter(_.isNode) else lhs
    val rhsWithAnyNode = if (rhs.contains(AnyNode)) (rhs - AnyNode) ++ lhs.filter(_.isNode) else rhs
    lhsWithAnyNode -> rhsWithAnyNode
  }

  def intersect(other: CypherType): CypherType = {
    if (this == CTAny) {
      other
    } else if (other == CTAny) {
      this
    } else if (this == other) {
      this
    } else {
      val (expanded, otherExpanded) = {
        if (isNode || isRelationship) {
          (expandAnyRelationship _).tupled(expandAnyNode(alternatives, other.alternatives))
        } else {
          alternatives -> other.alternatives
        }
      }
      if (expanded.subsetOf(otherExpanded)) {
        this
      } else if (otherExpanded.subsetOf(expanded)) {
        this
      } else {
        UnionType(expanded intersect otherExpanded)
      }
    }
  }

  def union(other: CypherType): CypherType = {
    if (this == CTAny || other == CTAny) {
      CTAny
    } else if (this == other) {
      this
    } else {
      UnionType(alternatives union other.alternatives)
    }
  }

  def isNullable: Boolean = {
    alternatives.contains(CTNull)
  }

  def nullable: CypherType = {
    if (isNullable) {
      this
    } else {
      UnionType(alternatives + CTNull)
    }
  }

  def name: String = alternatives.map(_.name).toSeq.sorted.mkString("|")

  override def toString: String = s"[$name]"

  /**
    * Ensures that basic types equal union types which contain a set with only that basic type.
    */
  override def equals(other: Any): Boolean = {
    this match {
      case bLhs: BasicType =>
        other match {
          case bRhs: BasicType =>
            bLhs.productArity == bRhs.productArity &&
              bLhs.productPrefix == bRhs.productPrefix &&
              bLhs.productIterator.sameElements(bRhs.productIterator)
          case uRhs: UnionType => bLhs.alternatives == uRhs.alternatives
          case _ => false
        }
      case uLhs: UnionType =>
        other match {
          case bRhs: BasicType => bRhs.alternatives == uLhs.alternatives
          case uRhs: UnionType => uLhs.alternatives.equals(uRhs.alternatives)
          case _ => false
        }
    }
  }

  override def hashCode(): Int = alternatives.hashCode

  // TODO: Remove
  def meet(other: CypherType): CypherType = intersect(other)

  // TODO: Remove
  def join(other: CypherType): CypherType = union(other)

}

// BASIC
trait BasicType extends CypherType with Product {
  override val alternatives: Set[BasicType] = Set(this)

  override val hashCode: Int = super.hashCode()

  override def name: String = getClass.getSimpleName.filterNot(_ == '$')
}

case object CTBoolean extends BasicType

case object CTString extends BasicType

case object CTInteger extends BasicType

case object CTFloat extends BasicType

case object CTNull extends BasicType

case object CTVoid extends BasicType {
  override val alternatives: Set[BasicType] = Set.empty
}

case object CTAny extends BasicType

trait CTEntity extends BasicType

object CTNode {

  def apply(labels: String*): CTNode = {
    CTNode(labels.toSet)
  }
}

case class CTNode(labels: Set[String]) extends CTEntity {

  override def name: String = s"CTNode(${labels.toSeq.sorted.mkString(", ")})"

  override def isNode: Boolean = true

}

object CTRelationship {

  def apply(relType: String, relTypes: String*): CypherType = {
    CTRelationship(relTypes.toSet + relType)
  }

  def apply(relTypes: Set[String]): CypherType = {
    if (relTypes.isEmpty) {
      CTRelationship()
    } else {
      relTypes.tail.map(e => CTRelationship(Some(e))).foldLeft(CTRelationship(Some(relTypes.head)): CypherType) { case (t, n) =>
        t.union(n)
      }
    }
  }
}

case class CTRelationship(relType: Option[String] = None) extends CTEntity {

  override def name: String = s"CTRelationship($relType)"

  override def isRelationship: Boolean = true

}

case class CTList(elementType: CypherType) extends BasicType {
  override def name: String = s"CTList$elementType"
}

// UNION
case class UnionType(either: Set[BasicType]) extends CypherType {

  override val alternatives: Set[BasicType] = {
    val canonicalWithAnyNode = if (either.contains(AnyNode)) {
      either.filterNot(e => e.isNode && e != AnyNode)
    } else {
      either
    }
    val canonicalWithAnyRel = if (canonicalWithAnyNode.contains(AnyRelationship)) {
      canonicalWithAnyNode.filterNot(e => e.isRelationship && e != AnyRelationship)
    } else {
      canonicalWithAnyNode
    }
    canonicalWithAnyRel
  }

  override val isNode: Boolean = alternatives.exists(_.isNode)

  override val isRelationship: Boolean = alternatives.exists(_.isRelationship)

}
