package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.types.CypherType.CTVoid
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._
import CypherType._
import org.opencypher.okapi.api.graph.QualifiedGraphName

import scala.language.postfixOps

object CypherType {

  val CTNumber: CypherType = CTInteger union CTFloat

  val CTVoid: CypherType = Union()

  val AnyList: CypherType = CTList().nullable

  val AnyMap: CypherType = CTMap().nullable

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
        case CypherMap(elements) => CTMap(elements.values.map(_.cypherType).foldLeft[CypherType](CTVoid)(_ union _))
        case CypherList(elements) => CTList(elements.map(_.cypherType).foldLeft[CypherType](CTVoid)(_ union _))
      }
    }
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x union y
  }

  implicit def rw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.name, s => parse(s))

  case object CTBoolean extends CypherType

  case object CTString extends CypherType

  case object CTInteger extends CypherType

  case object CTFloat extends CypherType

  case object CTNull extends CypherType {

    override def material: CypherType = CTVoid

  }

  case object CTPath extends CypherType

  case object CTAny extends CypherType with CTNode {

    override def intersect(other: CTNode): CTNode = other

    override def labels: Set[String] = Set.empty
  }

  trait CTEntity extends CypherType

  object CTNode {

    def apply(labels: String*): CTNode = {
      val ls = labels.toList
      ls match {
        case Nil => AnyNode
        case h :: Nil => CTLabel(h)
        case h :: t => t.foldLeft(CTLabel(h): CTNode) { case (ct, l) =>
          ct.intersect(CTLabel(l))
        }
      }
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case n: CTNode if n.subTypeOf(AnyNode) => Some(n.labels)
      case _ => None
    }

  }

  sealed trait CTNode extends CTEntity {

    def intersect(other: CTNode): CTNode

    def labels: Set[String]

  }

  case object AnyNode extends CTNode {

    override def intersect(other: CTNode): CTNode = other

    override def labels: Set[String] = Set.empty

  }

  case class CTLabel(label: String) extends CTNode {

    override def name: String = s"CTNode($label)"

    override def subTypeOf(other: CypherType): Boolean = {
      other match {
        case AnyNode => true
        case _ => super.subTypeOf(other)
      }
    }

    override def intersect(other: CTNode): CTNode = {
      other match {
        case CTAny => this
        case AnyNode => this
        case n: CTLabel => Intersection(this, n)
        case Union(ors) => Union(ors.map(_.intersect(this)))
        case Intersection(ands) => Intersection((ands + this).toSeq: _*)
      }
    }

    override def labels: Set[String] = Set(label)

  }

  object CTRelationship {

    def apply(relType: String, relTypes: String*): CTRelationship = {
      CTRelationship(relTypes.toSet + relType)
    }

    def apply(relTypes: Set[String]): CTRelationship = {
      if (relTypes.isEmpty) {
        AnyRelationship
      } else {
        relTypes.tail.map(e => CTRelType(e)).foldLeft(CTRelType(relTypes.head): CTRelationship) { case (t, n) =>
          t.union(n)
        }
      }
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case n: CTRelationship if n.subTypeOf(AnyRelationship) => Some(n.relTypes)
      case _ => None
    }

  }

  sealed trait CTRelationship extends CTEntity {

    override def subTypeOf(other: CypherType): Boolean = {
      other match {
        case AnyRelationship => true
        case _ => super.subTypeOf(other)
      }
    }

    def union(other: CTRelationship): CTRelationship = {
      other match {
        case AnyRelationship => AnyRelationship
        case _ =>
          val rs = (relTypes ++ other.relTypes).map(CTRelType).toList
          rs match {
            case Nil => AnyRelationship
            case r :: Nil => r
            case _ => Union(rs.toSet)
          }
      }
    }

    def relTypes: Set[String]

  }

  case object AnyRelationship extends CTRelationship {

    override def relTypes: Set[String] = Set.empty

    override def union(other: CTRelationship): CTRelationship = this

  }

  case class CTRelType(relType: String) extends CTRelationship {

    override def name: String = s"CTRelationship($relType)"

    override def relTypes: Set[String] = Set(relType)

  }

  case class CTMap(elementType: CypherType = CTAny) extends CypherType {

    override def subTypeOf(other: CypherType): Boolean = {
      other match {
        case CTMap(otherElementType) => elementType.subTypeOf(otherElementType)
        case _ => super.subTypeOf(other)
      }
    }

    override def intersect(other: CypherType): CypherType = {
      other match {
        case CTMap(otherElementType) => CTMap(elementType.intersect(otherElementType))
        case _ => super.intersect(other)
      }
    }

    override def maybeElementType: Option[CypherType] = {
      Some(elementType)
    }

  }

  case class CTList(elementType: CypherType = CTAny) extends CypherType {

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

    override def maybeElementType: Option[CypherType] = {
      Some(elementType)
    }

    override def name: String = s"CTList(${elementType.name})"

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

    override def toString: String = s"Intersection(${ands.mkString(", ")})"

    override def labels: Set[String] = ands.flatMap(_.labels)

    override def maybeElementType: Option[CypherType] = {
      val elementTypes = ands.map(_.maybeElementType)
      if (elementTypes.forall(_.nonEmpty)) {
        Some(elementTypes.flatten.foldLeft(CTAny: CypherType)(_ intersect _))
      } else {
        None
      }
    }

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

  case class Union(ors: Set[CypherType]) extends CypherType with CTRelationship with CTNode {
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

    override def toString: String = s"Union(${ors.mkString(", ")})"

    override def relTypes: Set[String] = {
      if (ors.contains(AnyRelationship)) Set.empty
      else ors.collect { case r: CTRelationship => r.relTypes }.flatten
    }

    override def maybeElementType: Option[CypherType] = {
      val elementTypes = ors.map(_.maybeElementType)
      if (elementTypes.exists(_.nonEmpty)) {
        Some(elementTypes.flatten.foldLeft(CTVoid)(_ union _))
      } else {
        None
      }
    }

    override def intersect(other: CTNode): CTNode = ???

    override def labels: Set[String] = {
      if (ors.contains(AnyNode)) Set.empty
      else ors.collect { case n: CTNode => n.labels }.reduce(_ ++ _)
    }
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


}

trait CypherType {

  def graph: Option[QualifiedGraphName] = None

  def withGraph(qgn: QualifiedGraphName): CypherType = this

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

  def setNullable(b: Boolean): CypherType = {
    if (b) nullable else material
  }

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

  def maybeElementType: Option[CypherType] = None

  def name: String = getClass.getSimpleName.filterNot(_ == '$')

  def pretty: String = s"[$name]"

}
