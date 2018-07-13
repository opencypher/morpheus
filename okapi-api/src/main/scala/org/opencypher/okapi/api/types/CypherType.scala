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
import org.opencypher.okapi.api.types.CypherType.{CTIntersection, CTNode, CTNull, CTRelationship, CTUnion, CTVoid}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import upickle.default._

import scala.language.postfixOps

object CypherType {

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

  val CTNumber: CypherType = CTInteger union CTFloat

  val CTAnyList: CypherType = CTList()

  val CTAnyMap: CypherType = CTMap()

  case object CTAny extends CypherType with AnyType[CypherType] {
    override def nullable: CypherType = this
  }

  case object CTVoid extends CypherType with NothingType[CypherType] {
    override def nullable: CypherType = CTNull
  }

  case class CTUnion(ors: Set[CypherType]) extends CypherType with UnionType[CypherType] {

//    override val children: Array[CypherType] = ors.toArray
//
//    override def withNewChildren(newChildren: Array[CypherType]): CypherType = CTUnion(newChildren.toSet)

    override def isNullable: Boolean = ors.exists(_.subTypeOf(CTNull))

    override def material: CypherType = {
      if (isNullable) newUnion(ors - CTNull) else this
    }

  }

  object CTUnion {

    def apply(ors: CypherType*): CTUnion = {
      CTUnion(ors.toSet)
    }

  }

  case class CTIntersection(ands: Set[CypherType]) extends CypherType with IntersectionType[CypherType] {
    override def isNullable: Boolean = ands.forall(_.subTypeOf(CTNull))

//    override val children: Array[CypherType] = ands.toArray
//
//    override def withNewChildren(newChildren: Array[CypherType]): CypherType = CTIntersection(newChildren.toSet)

  }

  object CTIntersection {

    def apply(ands: CypherType*): CTIntersection = {
      CTIntersection(ands.toSet)
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

  trait CTEntity extends CypherType

  object CTNode {

    def apply(labels: String*): CypherType = {
      CTNode(labels.toSet)
    }

    def apply(labels: Set[String]): CypherType = {
      val ls = labels.toList
      ls match {
        case Nil => CTNoLabel
        case h :: Nil => CTLabel(h)
        case h :: t => t.foldLeft(CTLabel(h): CypherType) { case (ct, l) =>
          ct intersect CTLabel(l)
        }
      }
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case CTNoLabel => throw UnsupportedOperationException("Not possible to retrieve labels from NoLabelNode")
      case CTAnyNode => Some(Set.empty)
      case CTLabel(l) => Some(Set(l))
      case CTUnion(ors) =>
        if (ors.contains(CTAnyNode)) Some(Set.empty)
        else ors.collectFirst { case CTNode(ls) => ls }
      case CTIntersection(ands) =>
        val labels = ands.collect { case CTNode(ls) => ls }.flatten
        if (labels.isEmpty) None
        else Some(labels)
      case _ => None
    }

  }

  case object CTNoLabel extends CTNode

  sealed trait CTNode extends CTEntity {

    override def canIntersect(other: CypherType): Boolean = other match {
      case CTNoLabel => false
      case _: CTProperty => true
      case _: CTLabel => true
      case _ => super.canIntersect(other)
    }

  }

  case object CTAnyNode extends CTNode {

    override def intersect(other: CypherType, tryReverseDirection: Boolean = true): CypherType = other match {
      case n: CTNode => n
      case _ => super.intersect(other, tryReverseDirection)
    }

  }

  case class CTLabel(label: String) extends CTNode {

    override def subTypeOf(other: CypherType): Boolean = {
      other match {
        case CTAnyNode => true
        case _ => super.subTypeOf(other)
      }
    }

    override def labels: Set[String] = Set(label)

    override def name: String = s"LABEL($label)"

  }

  object CTRelationship {

    def apply(relTypes: String*): CypherType = {
      CTRelationship(relTypes.toSet)
    }

    def apply(relTypes: Set[String]): CypherType = {
      if (relTypes.isEmpty) {
        CTAnyRelationship
      } else {
        relTypes.tail.map(e => CTRelType(e)).foldLeft(CTRelType(relTypes.head): CypherType) { case (t, n) =>
          t union n
        }
      }
    }

    def unapply(v: CypherType): Option[Set[String]] = v match {
      case CTRelType(r) => Some(Set(r))
      case CTAnyRelationship => Some(Set.empty)
      case CTUnion(ors) =>
        if (ors.contains(CTAnyRelationship)) {
          Some(Set.empty)
        } else {
          val relTypes = ors.collect { case CTRelType(r) => r }
          if (relTypes.isEmpty) {
            None
          } else {
            Some(relTypes)
          }
        }
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

    def relTypes: Set[String]

  }

  case object CTAnyRelationship extends CTRelationship

  case class CTRelType(relType: String) extends CTRelationship {

    override def relTypes: Set[String] = Set(relType)

    override def name: String = s"RELTYPE($relType)"

  }

  case class CTProperty(key: String, elementType: CypherType = CTAny) extends CypherType with ContainerType[CypherType] {

    override def isContainerSubType(other: CypherType): Boolean = other match {
      case CTProperty(otherKey, otherElementType) => key == otherKey && elementType.subTypeOf(otherElementType)
      case _ => false
    }

    override def intersectContainer(other: CypherType): Option[CypherType] = other match {
      case CTProperty(otherKey, otherElementType) if key == otherKey =>
        Some(copy(elementType = elementType.intersect(otherElementType)))
      case _ => None
    }

    override def canIntersect(other: CypherType): Boolean = other match {
      case CTNoLabel => true
      case CTAnyNode => true
      case _: CTProperty => true
      case _: CTLabel => true
      case _ => super.canIntersect(other)
    }

    override def name: String = s"PROPERTY($key)"

    override protected def newInstance(newElementType: CypherType): CypherType = copy(elementType = newElementType)

  }

  case class CTMap(elementType: CypherType = CTAny) extends CypherType with ContainerType[CypherType] {

    override def canIntersect(other: CypherType): Boolean = other match {
      case CTMap(_) => true
      case _ => super.canIntersect(other)
    }

    override def name: String = s"MAP(${elementType.name})"

    override protected  def newInstance(newElementType: CypherType): CypherType = copy(elementType = newElementType)

  }

  case class CTList(elementType: CypherType = CTAny) extends CypherType with ContainerType[CypherType] {

    override def canIntersect(other: CypherType): Boolean = other match {
      case CTList(_) => true
      case _ => super.canIntersect(other)
    }

    override def name: String = s"LIST(${elementType.name})"

    override protected def newInstance(newElementType: CypherType): CypherType = copy(elementType = newElementType)

  }

  // TODO: Use new type names for schema version 2, use legacy type names for schema version 1
  implicit def rw: ReadWriter[CypherType] = {

    import LegacyNames._

    readwriter[String].bimap[CypherType](_.legacyName, s => fromLegacyName(s).get)
  }

  implicit val joinMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    override def empty: CypherType = CTVoid

    override def combine(x: CypherType, y: CypherType): CypherType = x union y
  }

}

abstract class CypherType extends Type[CypherType] {

  // TODO: Remove
  def graph: Option[QualifiedGraphName] = None

  // TODO: Remove
  def withGraph(qgn: QualifiedGraphName): CypherType = this

  def labels: Set[String] = this match {
    case CTNode(labels) => labels
    case _ => throw UnsupportedOperationException("Accessing labels of something that is not a node.")
  }

  def relTypes: Set[String] = this match {
    case CTRelationship(relTypes) => relTypes
    case _ => throw UnsupportedOperationException("Accessing relationship types of something that is not a relationship.")
  }

  def maybeElementType: Option[CypherType] = this match {
    case c: ContainerType[CypherType] => Some(c.elementType)
    case CTUnion(ors) => ors.collectFirst { case c: ContainerType[CypherType] => c.elementType }
    case _ => None
  }

  def material: CypherType = this

  def nullable: CypherType = {
    if (isNullable) {
      this
    } else {
      this union CTNull
    }
  }

  def isNullable: Boolean = false

  override def name: String = {
    val basicName = getClass.getSimpleName.filterNot(_ == '$').drop(2).toUpperCase
    if (basicName.length > 3 && basicName.startsWith("ANY")) {
      basicName.drop(3)
    } else {
      basicName
    }
  }

  override def toString: String = name

  override protected def newUnion(ors: Set[CypherType]): CypherType = CTUnion(ors)

  override protected def newIntersection(ands: Set[CypherType]): CypherType = CTIntersection(ands)

  override protected def newNothing: CypherType = CTVoid

}
