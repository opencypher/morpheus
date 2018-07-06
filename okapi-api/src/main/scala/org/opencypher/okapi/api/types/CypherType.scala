package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._

import scala.language.postfixOps

object CypherType {

  val CTNumber: CypherType = CTInteger union CTFloat

  def anyNode(implicit schema: Schema): CypherType = CTNode.any

  def anyRelationship(implicit schema: Schema): CypherType = CTRelationship.any

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
        case CypherList(l) => CTList(l.map(_.cypherType).foldLeft[CypherType](CTVoid)(_.join(_)))
      }
    }
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x join y
  }

  implicit def rw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => parse(s))

}

trait CypherType {

  def alternatives: Set[CypherType]

  def material: Set[CypherType] = alternatives.filterNot(_ == CTNull)

  def subTypeOf(other: CypherType): Boolean = {
    other == CTAny || this == other || alternatives.subsetOf(other.alternatives)
  }

  def superTypeOf(other: CypherType): Boolean = {
    this == CTAny || this == other || other.alternatives.subsetOf(alternatives)
  }

  def intersect(other: CypherType): CypherType = {
    if (this == CTAny) {
      other
    } else if (other == CTAny) {
      this
    } else if (this == other) {
      this
    } else if (alternatives.contains(other)) {
      other
    } else if (other.alternatives.contains(this)) {
      this
    } else {
      UnionType(alternatives intersect other.alternatives)
    }
  }

  // TODO: Remove
  def meet(other: CypherType): CypherType = intersect(other)

  def union(other: CypherType): CypherType = {
    if (this == CTAny || other == CTAny) {
      CTAny
    } else if (this == other) {
      this
    } else if (alternatives.contains(other)) {
      this
    } else if (other.alternatives.contains(this)) {
      other
    } else {
      UnionType(alternatives union other.alternatives)
    }
  }

  // TODO: Remove
  def join(other: CypherType): CypherType = union(other)

  def isNullable: Boolean = {
    alternatives.contains(CTNull)
  }

  def nullable: CypherType = {
    if (this.alternatives.contains(CTNull)) {
      this
    } else {
      UnionType(alternatives + CTNull)
    }
  }

  def name: String = alternatives.map(_.name).toSeq.sorted.mkString("|")

  override def toString: String = s"[$name]"

}

// BASIC
trait BasicType extends CypherType {
  override def alternatives: Set[CypherType] = Set(this)

  override def name: String = getClass.getSimpleName.filterNot(_ == '$')
}

case object CTBoolean extends BasicType

case object CTString extends BasicType

case object CTInteger extends BasicType

case object CTFloat extends BasicType

case object CTNull extends BasicType

case object CTVoid extends BasicType {
  override def alternatives: Set[CypherType] = Set.empty
}

case object CTAny extends BasicType

trait CTEntity extends BasicType

object CTNode {

  def any(implicit schema: Schema): CypherType = {
    UnionType(schema.allLabelCombinations.map(CTNode(_)))
  }

  def apply(labels: String*)(implicit schema: Schema): CypherType = {
    if (labels.isEmpty) {
      CTNode(Set.empty[String])
    } else {
      val requiredLabels = labels.toSet
      val allPossibleCombos = schema.allLabelCombinations.filter(requiredLabels.subsetOf)
      UnionType(allPossibleCombos.map { ls: Set[String] => CTNode(ls) })
    }
  }
}

case class CTNode(labels: Set[String]) extends CTEntity {

  override def name: String = s"CTNode(${labels.toSeq.sorted.mkString(", ")})"

}

object CTRelationship {

  def any(implicit schema: Schema): CypherType = {
    CTRelationship(schema.relationshipTypes)
  }

  def apply(relType: String, relTypes: String*)(implicit schema: Schema): CypherType = {
    CTRelationship(relTypes.toSet + relType)
  }

  def apply(relTypes: Set[String])(implicit schema: Schema): CypherType = {
    val possibleTypes = schema.relationshipTypes intersect relTypes
    if (possibleTypes.isEmpty) {
      CTVoid
    } else {
      possibleTypes.tail.map(e => CTRelationship(e)).foldLeft(CTRelationship(possibleTypes.head): CypherType) { case (t, n) =>
        t.union(n)
      }
    }
  }
}

case class CTRelationship(relType: String) extends CTEntity {

  override def name: String = s"CTRelationship($relType)"

}

case class CTList(elementType: CypherType) extends BasicType {
  override def name: String = s"CTList$elementType"
}

// UNION
case class UnionType(alternatives: Set[CypherType]) extends CypherType
