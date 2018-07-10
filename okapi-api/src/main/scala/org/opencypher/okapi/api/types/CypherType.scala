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
package org.opencypher.okapi.api.types

import cats.Monoid
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CypherType.{CTVoid, _}
import org.opencypher.okapi.api.value.CypherValue._
import upickle.default._

import scala.language.postfixOps

// TODO: Turn expensive computations into lazy vals
object CypherType {

  // TODO: Use new type names
  implicit def rw: ReadWriter[CypherType] = {
    import LegacyNames._
    readwriter[String].bimap[CypherType](_.legacyName, s => fromLegacyName(s).get)
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x union y
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

    override def name: String = s"LABEL($label)"

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

    override def name: String = s"RELTYPE($relType)"

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

    override def name: String = {
      if (ands.isEmpty) {
        "NO-LABEL"
      } else {
        val inner = ands.map(_.name).toSeq.sorted.mkString("&")
        if (ands.size > 1) {
          s"($inner)"
        } else {
          inner
        }
      }
    }

    override def toString: String = s"CTIntersection(${ands.mkString(", ")})"

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

    override def name: String = {
      if (ors.isEmpty) {
        "VOID"
      } else if (isNullable) {
        if (ors.size > 2) {
          s"(${material.name})?"
        } else {
          s"${material.name}?"
        }
      } else {
        ors.map(_.name).toSeq.sorted.mkString("|")
      }
    }

    override def toString: String = s"CTUnion(${ors.mkString(", ")})"

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
      case i: CTIntersection => i.ands.nonEmpty && i.ands.forall(this.subTypeOf)
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

}
