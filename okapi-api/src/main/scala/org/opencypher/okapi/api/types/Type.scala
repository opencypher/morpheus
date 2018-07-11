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

import org.opencypher.okapi.trees.AbstractTreeNode

import scala.reflect.ClassTag

/**
  * Abstract class instead of trait in order to support `ClassTag`
  */
abstract class Type[T <: Type[T] : ClassTag] extends AbstractTreeNode[T] {
  self: T =>

  def name: String

  protected def newUnion(ors: Set[T]): T

  protected def newIntersection(ands: Set[T]): T

  protected def newNothing: T

  private[types] def flattenAndUnion(ors: T*): T = {
    flattenAndUnion(ors.toSet)
  }

  private[types] def flattenAndUnion(ors: Set[T]): T = {
    val flattened = UnionType.flatten(ors)
    if (flattened.isEmpty) {
      newNothing
    } else if (flattened.size == 1) {
      flattened.head
    } else {
      newUnion(flattened)
    }
  }

  private[types] def flattenAndIntersect(ands: T*): T = {
    flattenAndIntersect(ands.toSet)
  }

  private[types] def flattenAndIntersect(ands: Set[T]): T = {
    val flattened = IntersectionType.flatten(ands)
    if (flattened.isEmpty) {
      newNothing
    } else if (flattened.size == 1) {
      flattened.head
    } else {
      newIntersection(flattened)
    }
  }

  def possibleTypes: Set[T] = Set(this)

  def couldBeSubTypeOf(other: T): Boolean = possibleTypes.exists(_.subTypeOf(other))

  def subTypeOf(other: T): Boolean = {
    this == other || {
      other match {
        case _: AnyType[T] => true
        case _: NothingType[T] => this.isInstanceOf[NothingType[T]]
        case u: UnionType[T] => u.ors.exists(this.subTypeOf)
        case i: IntersectionType[T] => i.ands.forall(this.subTypeOf)
        case _ => false
      }
    }
  }

  def superTypeOf(other: T): Boolean = {
    this == other || other.subTypeOf(this)
  }

  def |(other: T): T = union(other)

  def union(other: T): T = {
    if (subTypeOf(other)) other
    else if (other.subTypeOf(this)) this
    else flattenAndUnion(this, other)
  }

  def canIntersect(other: T): Boolean = false

  def &(other: T): T = intersect(other)

  def intersect(other: T, tryReverseDirection: Boolean = true): T = {
    if (subTypeOf(other)) this
    else if (other.subTypeOf(this)) other
    else {
      other match {
        case i: IntersectionType[T] if i.canIntersect(other) => flattenAndIntersect(i.ands + this)
        case u: UnionType[T] if u.canIntersect(other) => flattenAndUnion(u.ors.map(_.intersect(this)))
        case _ if tryReverseDirection => other.intersect(this, tryReverseDirection = false)
        case _ if canIntersect(other) || other.canIntersect(this) => flattenAndIntersect(this, other)
        case _ => newNothing
      }
    }
  }

}

trait ContainerType[T <: Type[T]] extends Type[T] {
  self: T =>

  def elementType: T

  override def subTypeOf(other: T): Boolean = isContainerSubType(other) || super.subTypeOf(other)

  override def intersect(other: T, tryReverseDirection: Boolean = true): T = other match {
    case c: ContainerType[T] =>
      val maybeLeftWithRight = if (canIntersect(other)) intersectContainer(other) else None
      maybeLeftWithRight.getOrElse {
        val maybeRightWithLeft = if (other.canIntersect(this)) c.intersectContainer(this) else None
        maybeRightWithLeft.getOrElse(super.intersect(other, tryReverseDirection = false))
      }
    case _ => super.intersect(other, tryReverseDirection)
  }

  protected def isContainerSubType(other: T): Boolean = other match {
    case c: ContainerType[T] if c.getClass.isAssignableFrom(getClass) => elementType.subTypeOf(c.elementType)
    case _ => false
  }

  protected def intersectContainer(other: T): Option[T] = other match {
    case c: ContainerType[T] =>
      if (isContainerSubType(other)) {
        Some(copyWithNewElementType(elementType.intersect(c.elementType)))
      } else if (c.isContainerSubType(this)) {
        Some(c.copyWithNewElementType(elementType.intersect(c.elementType)))
      } else {
         if (getClass == other.getClass) {
           Some(copyWithNewElementType(newNothing))
         } else {
           None
         }
      }
    case _ => None
  }

  protected def copyWithNewElementType(newElementType: T): T

}

trait AnyType[T <: Type[T]] extends Type[T] {
  self: T =>

  override def subTypeOf(other: T): Boolean = other == this

  override def union(other: T): T = this

  override def canIntersect(other: T): Boolean = true

  override def intersect(other: T, tryReverseDirection: Boolean = true): T = other

}

trait NothingType[T <: Type[T]] extends Type[T] {
  self: T =>

  override def subTypeOf(other: T): Boolean = true

  override def union(other: T): T = other

  override def canIntersect(other: T): Boolean = true

  override def intersect(other: T, tryReverseDirection: Boolean = true): T = this

}

trait UnionType[T <: Type[T]] extends Type[T] {
  self: T =>

  def ors: Set[T]

  override def canIntersect(other: T): Boolean = ors.forall(or => or.canIntersect(other) || other.canIntersect(or))

  override def possibleTypes: Set[T] = ors

  override def subTypeOf(other: T): Boolean = other match {
    case u: UnionType[T] => ors.forall(or => u.ors.exists(or.subTypeOf))
    case i: IntersectionType[T] => ors.forall(or => i.ands.forall(or.subTypeOf))
    case _ => super.subTypeOf(other)
  }

  override def name: String = ors.map(_.name).toSeq.sorted.mkString("[", "|", "]")

  override def toString: String = s"${getClass.getSimpleName}(${ors.map(_.toString).toSeq.sorted.mkString(", ")})"

}

object UnionType {

  def flatten[T <: Type[T]](ors: Set[T]): Set[T] = {
    ors.flatMap {
      case u: UnionType[T] => u.ors
      case other => Set(other)
    }
  }

}

trait IntersectionType[T <: Type[T]] extends Type[T] {
  self: T =>

  def ands: Set[T]

  override def canIntersect(other: T): Boolean = ands.forall(and => and.canIntersect(other) || other.canIntersect(and))

  override def subTypeOf(other: T): Boolean = other match {
    case u: UnionType[T] => u.ors.exists(or => ands.forall(_ superTypeOf or))
    case i: IntersectionType[T] => i.ands.forall(otherAnd => ands.exists(_ subTypeOf otherAnd))
    case _ if ands.exists(_.subTypeOf(other)) => true
    case _ => super.subTypeOf(other)
  }

  override def name: String = ands.map(_.name).toSeq.sorted.mkString("[", "&", "]")

  override def toString: String = s"${getClass.getSimpleName}(${ands.map(_.toString).toSeq.sorted.mkString(", ")})"

}

object IntersectionType {

  def flatten[T <: Type[T]](ands: Set[T]): Set[T] = {
    ands.flatMap {
      case i: IntersectionType[T] => i.ands
      case other => Set(other)
    }
  }

}
