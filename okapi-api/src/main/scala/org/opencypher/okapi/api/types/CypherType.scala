package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.types.CypherType.CTVoid
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._
import CypherType._
import org.opencypher.okapi.api.graph.QualifiedGraphName

import scala.language.postfixOps

// TODO: Turn expensive computations into lazy vals
object CypherType {

  /**
    * Parses the name of CypherType into the actual CypherType object.
    *
    * @param name string representation of the CypherType
    * @return
    * @see {{{org.opencypher.okapi.api.types.CypherType#name}}}
    */
  def fromLegacyName(name: String): Option[CypherType] = {
    def extractLabels(s: String, typ: String, sep: String): Set[String] = {
      val regex = s"""$typ\\(:(.+)\\).?""".r
      s match {
        case regex(l) => l.split(sep).toSet
        case _ => Set.empty[String]
      }
    }

    val noneNullType: Option[CypherType] = name match {
      case "STRING"  | "STRING?"  => Some(CTString)
      case "INTEGER" | "INTEGER?" => Some(CTInteger)
      case "FLOAT"   | "FLOAT?"   => Some(CTFloat)
      case "NUMBER"  | "NUMBER?"  => Some(CTNumber)
      case "BOOLEAN" | "BOOLEAN?" => Some(CTBoolean)
      case "ANY"     | "ANY?"     => Some(CTAny)
      case "VOID"    | "VOID?"    => Some(CTVoid)
      case "NULL"    | "NULL?"    => Some(CTNull)
      case "MAP"     | "MAP?"     => Some(CTAnyMap)
      case "PATH"    | "PATH?"    => Some(CTPath)
      case "?"       | "??"       => Some(CTAny)

      case node if node.startsWith("NODE") =>
        Some(CTNode(extractLabels(node, "NODE", ":")))

      case rel if rel.startsWith("RELATIONSHIP") =>
        Some(CTRelationship(extractLabels(rel, "RELATIONSHIP", """\|""")))

      case list if list.startsWith("LIST") =>
        CypherType
          .fromLegacyName(list.replaceFirst("""LIST\?? OF """,""))
          .map(CTList)

      case _ => None
    }

    noneNullType.map(ct => if (name == ct.legacyName) ct else ct.nullable)
  }

  val CTNumber: CTUnion = CTUnion(Set(CTInteger, CTFloat))

  val CTNoLabelNode: CTNode = CTUnion(Set(CTIntersection()))

  val CTVoid: CypherType = CTUnion(Set.empty[CypherType])

  val CTAnyList: CypherType = CTList().nullable

  val CTAnyMap: CypherType = CTMap()

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

  implicit def rw: ReadWriter[CypherType] = readwriter[String].bimap[CypherType](_.legacyName, s => fromLegacyName(s).get)

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
        case Nil => CTAnyNode
        case h :: Nil => CTLabel(h)
        case h :: t => t.foldLeft(CTLabel(h): CTNode) { case (ct, l) =>
          ct.intersect(CTLabel(l))
        }
      }
    }

    def apply(labels: Set[String]): CTNode = {
      CTNode(labels.toSeq: _*)
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case n: CTNode if n.subTypeOf(CTAnyNode) => Some(n.labels)
      case _ => None
    }

  }

  sealed trait CTNode extends CTEntity {

    def intersect(other: CTNode): CTNode

    def labels: Set[String]

  }

  case object CTAnyNode extends CTNode {

    override def intersect(other: CTNode): CTNode = other

    override def labels: Set[String] = Set.empty

  }

  case class CTLabel(label: String) extends CTNode {

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
        case n: CTLabel => CTIntersection(this, n)
        case CTUnion(ors) => CTUnion(ors.map(_.intersect(this)))
        case CTIntersection(ands) => CTIntersection((ands + this).toSeq: _*)
      }
    }

    override def labels: Set[String] = Set(label)

    override def legacyName: String = s"NODE(:$label)"

  }

  object CTRelationship {

    def apply(relTypes: String*): CTRelationship = {
      if (relTypes.isEmpty) {
        CTAnyRelationship
      } else {
        relTypes.tail.map(e => CTRelType(e)).foldLeft(CTRelType(relTypes.head): CTRelationship) { case (t, n) =>
          t.union(n)
        }
      }
    }

    def apply(labels: Set[String]): CTRelationship = {
      CTRelationship(labels.toSeq: _*)
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case n: CTRelationship if n.subTypeOf(CTAnyRelationship) => Some(n.relTypes)
      case _ => None
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
          val rs = (relTypes ++ other.relTypes).map(CTRelType).toList
          rs match {
            case Nil => CTAnyRelationship
            case r :: Nil => r
            case _ => CTUnion(rs.toSet)
          }
      }
    }

    def relTypes: Set[String]

  }

  case object CTAnyRelationship extends CTRelationship {

    override def relTypes: Set[String] = Set.empty

    override def union(other: CTRelationship): CTRelationship = this

  }

  case class CTRelType(relType: String) extends CTRelationship {

    override def relTypes: Set[String] = Set(relType)

    override def legacyName: String = s"RELATIONSHIP(:$relType)"

  }

  case class CTMap(elementType: CypherType = CTAny) extends CypherType with Container {

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

  sealed trait Container {

    def elementType: CypherType

  }

  case class CTList(elementType: CypherType = CTAny) extends CypherType with Container {

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

    override def name: String = {
      s"LIST(${elementType.name})"
    }

    override def legacyName: String = {
      s"LIST OF ${elementType.legacyName}"
    }

  }

  object CTIntersection {

    def apply(ands: CTNode*): CTNode = {
      if (ands.size == 1) {
        ands.head
      } else {
        CTIntersection(ands.toSet)
      }
    }

  }

  case class CTIntersection(ands: Set[CTNode]) extends CypherType with CTNode {

    override def subTypeOf(other: CypherType): Boolean = {
      this == other || {
        other match {
          case CTUnion(otherOrs) => otherOrs.exists(this.subTypeOf)
          case CTIntersection(otherAnds) => ands.subsetOf(otherAnds)
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
        CTIntersection((ands + other).toSeq: _*)
      }
    }

    override def union(other: CypherType): CypherType = {
      other match {
        case CTIntersection(otherAnds) => CTIntersection(ands.intersect(otherAnds).toSeq: _*)
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

    override def legacyName: String = {
      if (subTypeOf(CTAnyNode)) {
        s"NODE(${labels.mkString(":", ":", "")})"
      } else {
        // Non-legacy type
        name
      }
    }

  }

  object CTUnion {

    def apply(ors: CypherType*): CypherType = {
      if (ors.size == 1) {
        ors.head
      } else {
        val flattenedOrs: Set[CypherType] = ors.toSet.flatMap { o: CypherType =>
          o match {
            case CTUnion(innerOrs) => innerOrs
            case other => Set(other)
          }
        }
        CTUnion(flattenedOrs)
      }
    }

  }

  case class CTUnion(ors: Set[CypherType]) extends CypherType with CTRelationship with CTNode {
    require(ors.forall(!_.isInstanceOf[CTUnion]), s"Nested unions are not allowed: ${ors.mkString(", ")}")

    override def subTypeOf(other: CypherType): Boolean = {
      this == other || {
        other match {
          case CTUnion(otherOrs) => ors.forall(or => otherOrs.exists(or.subTypeOf))
          case _ => ors.forall(_.subTypeOf(other))
        }
      }
    }

    override def material: CypherType = {
      if (!isNullable) {
        this
      } else {
        val m = ors - CTNull
        if (m.size == 1) {
          m.head
        } else {
          CTUnion(m)
        }
      }
    }

    override def nullable: CypherType = {
      if (isNullable) {
        this
      } else {
        CTUnion((ors + CTNull).toSeq: _*)
      }
    }

    override def name: String = ors.map(_.name).toSeq.sorted.mkString("|")

    override def toString: String = s"Union(${ors.mkString(", ")})"

    override def relTypes: Set[String] = {
      if (ors.contains(CTAnyRelationship)) Set.empty
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

    override def intersect(other: CTNode): CTNode = {
      if (other.subTypeOf(this)) {
        other
      } else if (this.subTypeOf(other)) {
        other
      } else {
        CTUnion(ors.map(_.intersect(other)))
      }
    }

    override def labels: Set[String] = {
      if (ors.contains(CTAnyNode)) Set.empty
      else ors.collect { case n: CTNode => n.labels }.reduce(_ ++ _)
    }

    override def legacyName: String = {
      val nullableSuffix = if (isNullable) "?" else ""
      if (this == CTVoid) {
        "VOID"
      } else if (this == CTNumber || this == CTNumber.nullable) {
        s"NUMBER$nullableSuffix"
      } else if (this == CTNoLabelNode) {
        "CTNode()"
      } else if (this == CTNoLabelNode.nullable) {
        s"CTNode()?"
      } else if (subTypeOf(CTAnyNode.nullable)) {
        if (labels.isEmpty) {
          s"NODE$nullableSuffix"
        } else {
          s"NODE(${labels.mkString(":", ":", "")})$nullableSuffix"
        }
      } else if (subTypeOf(CTAnyRelationship.nullable)) {
        if (relTypes.isEmpty) {
          s"RELATIONSHIP$nullableSuffix"
        } else {
          s"RELATIONSHIP(${relTypes.mkString(":", "|", "")})$nullableSuffix"
        }
      } else if (subTypeOf(CTAnyList.nullable)) {
        val elementType = ors.collect{case CTList(et) => et }.head
        s"LIST$nullableSuffix OF ${elementType.legacyName}"
      } else if (isNullable && ors.size == 2) {
        s"${material.legacyName}$nullableSuffix"
      } else if (!isNullable && ors.size == 1) {
        ors.head.legacyName
      } else {
        // Non-legacy type
        name
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
      CTUnion(this, other)
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
      case u: CTUnion => u.ors.exists(this.subTypeOf)
      case i: CTIntersection => i.ands.forall(this.subTypeOf)
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
      CTUnion(this, CTNull)
    }
  }

  def maybeElementType: Option[CypherType] = None

  def name: String = {
    val basicName = getClass.getSimpleName.filterNot(_ == '$').drop(2).toUpperCase
    if (basicName.length > 3 && basicName.startsWith("ANY")) {
      basicName.drop(3)
    } else {
      basicName
    }
  }

  def pretty: String = s"[$name]"

  def legacyName: String = name

}
