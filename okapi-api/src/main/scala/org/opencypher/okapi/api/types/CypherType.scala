package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.types.CypherType.CTVoid
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

  // In general, intersect is only supported when one type is a sub-type of the other.
  def intersect(other: CypherType): CypherType = {
    if (this.subTypeOf(other)) {
      this
    } else if (other.subTypeOf(this)) {
      other
    } else {
      CTVoid
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

  override def material: CypherType = CTVoid

}

case object CTAny extends CypherType with CTNode {

  override def intersect(other: CTNode): CTNode = other

  override def labelCombination: Set[String] = Set.empty
}

trait CTEntity extends CypherType

object CTNode {

  def apply(labels: String*): CTNode = {
    val ls = labels.toList
    ls match {
      case Nil => CTAnyNode
      case h :: Nil => CTNodeWithLabel(h)
      case h :: t => t.foldLeft(CTNodeWithLabel(h): CTNode) { case (ct, l) =>
        ct.intersect(CTNodeWithLabel(l))
      }
    }
  }

}

sealed trait CTNode extends CTEntity {

  def intersect(other: CTNode): CTNode

  def labelCombination: Set[String]

}

case object CTAnyNode extends CTNode {

  override def intersect(other: CTNode): CTNode = other

  override def labelCombination: Set[String] = Set.empty

}

case class CTNodeWithLabel(label: String) extends CTNode {

  override def name: String = s"CTNode($label)"

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTAnyNode => true
      case _ => super.subTypeOf(other)
    }
  }

  override def intersect(other: CTNode): CTNode = {
    other match {
      case CTAny => this
      case CTAnyNode => this
      case n: CTNodeWithLabel => Intersection(this, n)
      case Intersection(ands) => Intersection((ands + this).toSeq: _*)
    }
  }

  override def labelCombination: Set[String] = Set(label)

}

object CTRelationship {

  def apply(relType: String, relTypes: String*): CTRelationship = {
    CTRelationship(relTypes.toSet + relType)
  }

  def apply(relTypes: Set[String]): CTRelationship = {
    if (relTypes.isEmpty) {
      CTAnyRelationship
    } else {
      relTypes.tail.map(e => CTRelationshipWithType(e)).foldLeft(CTRelationshipWithType(relTypes.head): CTRelationship) { case (t, n) =>
        t.union(n)
      }
    }
  }
}

sealed trait CTRelationship extends CTEntity {

  override def subTypeOf(other: CypherType): Boolean = {
    other match {
      case CTAnyRelationship => true
      case _ => super.subTypeOf(other)
    }
  }

  def union(other: CTRelationship): CTRelationship = {
    other match {
      case CTAnyRelationship => CTAnyRelationship
      case _ =>
        val rs = (relTypes ++ other.relTypes).map(CTRelationshipWithType).toList
        rs match {
          case Nil => CTAnyRelationship
          case r :: Nil => r
          case _ => Union(rs.toSet)
        }
    }
  }

  def relTypes: Set[String]

}

case object CTAnyRelationship extends CTRelationship {

  override def relTypes: Set[String] = Set.empty

  override def union(other: CTRelationship): CTRelationship = this

}

case class CTRelationshipWithType(relType: String) extends CTRelationship {

  override def name: String = s"CTRelationship($relType)"

  override def isRelationship: Boolean = true

  override def relTypes: Set[String] = Set(relType)

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

case class Intersection(ands: Set[CTNode]) extends CypherType with CTNode {

  override def subTypeOf(other: CypherType): Boolean = {
    this == other || {
      other match {
        case Union(otherOrs) => otherOrs.exists(this.subTypeOf)
        case Intersection(otherAnds) => ands.subsetOf(otherAnds)
        case _ => ands.exists(_.subTypeOf(other))
      }
    }
  }

  override def intersect(other: CTNode): CTNode = {
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

  override def labelCombination: Set[String] = ands.flatMap(_.labelCombination)

}

object Intersection {

  def apply(ands: CTNode*): CTNode = {
    if (ands.size == 1) {
      ands.head
    } else {
      Intersection(ands.toSet)
    }
  }

}

case class Union(ors: Set[CypherType]) extends CypherType with CTRelationship {
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

  override def relTypes: Set[String] = ors.collect { case r: CTRelationship => r.relTypes }.flatten

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
