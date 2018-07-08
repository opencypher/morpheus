package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._

import scala.language.postfixOps

object CypherType {

  val CTNumber: CypherType = CTInteger union CTFloat

  val CTVoid: CypherType = Union()

  //case object CTVoid extends CypherType

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

  def intersect(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) {
      this
    } else if (other.subTypeOf(this)) {
      other
    } else {
      Intersection(this, other)
    }
  }

  def subTypeOf(other: CypherType): Boolean = this == other || {
    other match {
      case CTAny => true
      case u: Union => u.superTypeOf(this)
      case i: Intersection => i.subTypeOf(this)
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

  // TODO: Remove
  def meet(other: CypherType): CypherType = intersect(other)

  // TODO: Remove
  def join(other: CypherType): CypherType = union(other)

}

case object CTBoolean extends CypherType

case object CTString extends CypherType

case object CTInteger extends CypherType

case object CTFloat extends CypherType

case object CTNull extends CypherType {

  override def material: CypherType = CypherType.CTVoid

}

case object CTAny extends CypherType

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

trait CTNode extends CTEntity {

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTAnyNode => true
      case _ => super.subTypeOf(other)
    }
  }

}

case object CTAnyNode extends CTNode

case class CTNodeWithLabel(label: String) extends CTNode {

  override def name: String = s"CTNode($label)" //.toSeq.sorted.mkString(", ")

  override def isNode: Boolean = true

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
      this
    } else if (this.subTypeOf(other)) {
      other
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
  require(ors.forall(!_.isInstanceOf[Union]), s"Nested unions are not allowed, got ${ors.mkString(", ")}")

  override def subTypeOf(other: CypherType): Boolean = {
    this == other || ors.forall(_.subTypeOf(other))
  }

  override def superTypeOf(other: CypherType): Boolean = {
    this == other || {
      other match {
        case Union(otherOrs) => otherOrs.forall(otherOr => ors.exists(_.superTypeOf(otherOr)))
        case _ => ors.exists(_.superTypeOf(other))
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
