package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._

import scala.language.postfixOps

object CypherType {

  val CTNumber: CypherType = CTInteger union CTFloat

  val CTVoid: CypherType = Union()

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
        case CypherNode(_, labels, _) => CTNode(labels.toSeq: _*)
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

  def isNode = false

  def isRelationship = false

  def material: CypherType = this

  def union(other: CypherType): CypherType = {
    if (other.subTypeOf(this)) {
      this
    } else if (this.subTypeOf(other)) {
      other
    } else {
      Union(this, other)
    }
  }

  // Intersect is only supported between CTAny and CTNode
  def intersect(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) {
      this
    } else if (other.subTypeOf(this)) {
      other
    } else {
      CypherType.CTVoid
    }
  }

  def subTypeOf(other: CypherType): Boolean = this == other || {
    other match {
      case CTAny => true
      case u: Union => u.ors.exists(this.subTypeOf)
      case i: Intersection => i.ands.forall(this.subTypeOf)
      case _ => false
    }
  }

  def superTypeOf(other: CypherType): Boolean = this == other || other.subTypeOf(this)


  def isNullable: Boolean = {
    CTNull.subTypeOf(this)
  }

  def nullable: CypherType = {
    if (isNullable) {
      this
    } else {
      Union(this, CTNull)
    }
  }

  def name: String = getClass.getSimpleName.filterNot(_ == '$')

  override def toString: String = s"[$name]"

}

case object CTBoolean extends CypherType

case object CTString extends CypherType

case object CTInteger extends CypherType

case object CTFloat extends CypherType

case object CTNull extends CypherType {

  override def material: CypherType = CypherType.CTVoid

}

case object CTAny extends CypherType {

  override def intersect(other: CypherType): CypherType = other

}

trait CTEntity extends CypherType

object CTNode {

  def apply(labels: String*): CypherType = {
    val ls = labels.toList
    ls match {
      case Nil => CTAnyNode
      case h :: Nil => CTNodeWithLabel(h)
      case h :: t => t.foldLeft(CTNodeWithLabel(h): CypherType) { case (ct, l) =>
        ct.intersect(CTNodeWithLabel(l))
      }
    }
  }

}

trait CTNode extends CTEntity

case object CTAnyNode extends CTNode {

  override def intersect(other: CypherType): CypherType = {
    other match {
      case _: CTNode => other
      case _ => super.intersect(other)
    }
  }

}

case class CTNodeWithLabel(label: String) extends CTNode {

  override def name: String = s"CTNode($label)"

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTAnyNode => true
      case _ => super.subTypeOf(other)
    }
  }

  override def intersect(other: CypherType): CypherType = {
    other match {
      case CTAny => this
      case CTAnyNode => this
      case n: CTNodeWithLabel => Intersection(this, n)
      case _ => super.intersect(other)
    }
  }

}

object CTRelationship {

  def apply(relType: String, relTypes: String*): CypherType = {
    CTRelationship(relTypes.toSet + relType)
  }

  def apply(relTypes: Set[String]): CypherType = {
    if (relTypes.isEmpty) {
      CTAnyRelationship
    } else {
      relTypes.tail.map(e => CTRelationshipWithType(e)).foldLeft(CTRelationshipWithType(relTypes.head): CypherType) { case (t, n) =>
        t.union(n)
      }
    }
  }
}

trait CTRelationship extends CTEntity {

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTAnyRelationship => true
      case _ => super.subTypeOf(other)
    }
  }

}

case object CTAnyRelationship extends CTRelationship

case class CTRelationshipWithType(relType: String) extends CTRelationship {

  override def name: String = s"CTRelationship($relType)"

  override def isRelationship: Boolean = true

}

case class CTList(elementType: CypherType) extends CypherType {

  override def name: String = s"CTList$elementType"

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTList(otherElementType) => elementType.subTypeOf(otherElementType)
      case _ => super.subTypeOf(other)
    }
  }

  override def intersect(other: CypherType): CypherType = {
    other match {
      case CTList(otherElementType) => CTList(elementType.intersect(otherElementType))
      case _ => super.intersect(other)
    }
  }

}

case class Intersection(ands: Set[CypherType]) extends CypherType {

  override def subTypeOf(other: CypherType): Boolean = {
    this == other || {
      other match {
        case Union(otherOrs) => otherOrs.exists(this.subTypeOf)
        case Intersection(otherAnds) => ands.subsetOf(otherAnds)
        case _ => ands.exists(_.subTypeOf(other))
      }
    }
  }

  override def intersect(other: CypherType): CypherType = {
    if (other.subTypeOf(this)) {
      other
    } else if (this.subTypeOf(other)) {
      this
    } else {
      Intersection((ands + other).toSeq: _*)
    }
  }

  override def union(other: CypherType): CypherType = {
    other match {
      case Intersection(otherAnds) => Intersection(ands.intersect(otherAnds).toSeq: _*)
      case _ => super.union(other)
    }
  }

  override def name: String = ands.map(_.name).toSeq.sorted.mkString("&")

}

object Intersection {

  def apply(ands: CypherType*): CypherType = {
    if (ands.size == 1) {
      ands.head
    } else {
      Intersection(ands.toSet)
    }
  }

}

case class Union(ors: Set[CypherType]) extends CypherType {
  require(ors.forall(!_.isInstanceOf[Union]), s"Nested unions are not allowed: ${ors.mkString(", ")}")

  override def subTypeOf(other: CypherType): Boolean = {
    this == other || {
      other match {
        case Union(otherOrs) => ors.forall(or => otherOrs.exists(or.subTypeOf))
        case _ => ors.forall(_.subTypeOf(other))
      }
    }
  }

  override def material: CypherType = {
    if (!isNullable) {
      this
    } else {
      Union(ors - CTNull)
    }
  }

  override def nullable: CypherType = {
    if (isNullable) {
      this
    } else {
      Union((ors + CTNull).toSeq: _*)
    }
  }

  override def name: String = ors.map(_.name).toSeq.sorted.mkString("|")
}

object Union {

  def apply(ors: CypherType*): CypherType = {
    if (ors.size == 1) {
      ors.head
    } else {
      val flattenedOrs: Set[CypherType] = ors.toSet.flatMap { o: CypherType =>
        o match {
          case Union(innerOrs) => innerOrs
          case other => Set(other)
        }
      }
      Union(flattenedOrs)
    }
  }

}
