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

import scala.reflect.ClassTag

/**
  * Abstract class instead of trait in order to support `ClassTag`
  */
abstract class Type[T <: Type[T]: ClassTag] {
  self: T =>

  def name: String

  protected def newUnion(ors: Set[T]): T

  protected def newIntersection(ands: Set[T]): T

  protected def newNothing: T

  protected def union(ors: T*): T = {
    union(ors.toSet)
  }

  protected def union(ors: Set[T]): T = {
    val flattened = UnionType.flatten(ors)
    if (flattened.isEmpty) {
      newNothing
    } else if (flattened.size == 1) {
      flattened.head
    } else {
      newUnion(flattened)
    }
  }

  protected def intersect(ands: T*): T = {
    intersect(ands.toSet)
  }

  protected def intersect(ands: Set[T]): T = {
    val flattened = IntersectionType.flatten(ands)
    if (flattened.isEmpty) {
      newNothing
    } else if (flattened.size == 1) {
      flattened.head
    } else {
      newIntersection(flattened)
    }
  }

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

  def union(other: T): T = {
    if (subTypeOf(other)) other
    else if (other.subTypeOf(this)) this
    else union(this, other)
  }

  def canIntersect(other: T): Boolean = false

  def intersect(other: T): T = {
    if (subTypeOf(other)) this
    else if (other.subTypeOf(this)) other
    else {
      other match {
        case _: AnyType[T] => this
        case _: NothingType[T] => other
        case i: IntersectionType[T] if i.canIntersect(other) => intersect(i.ands.map(_.intersect(this)))
        case u: UnionType[T] if u.canIntersect(other) => union(u.ors.map(_.intersect(this)))
        case _ if canIntersect(other) || other.canIntersect(this) => intersect(this, other)
        case _ => newNothing
      }
    }
  }

}

trait ContainerType[T <: Type[T]] extends Type[T] {
  self: T =>

  def newInstance(et: T): T

  def elementType: T

  override def subTypeOf(other: T): Boolean = other match {
    case c: ContainerType[T] if c.getClass == this.getClass => elementType.subTypeOf(c.elementType)
    case _ => super.subTypeOf(other)
  }

  override def intersect(other: T): T = other match {
    case c: ContainerType[T] if c.getClass == this.getClass => newInstance(elementType.intersect(c.elementType))
    case _ => super.intersect(other)
  }

}

trait AnyType[T <: Type[T]] extends Type[T] {
  self: T =>

  override def subTypeOf(other: T): Boolean = other == this

  override def union(other: T): T = this

  override def canIntersect(other: T): Boolean = true

  override def intersect(other: T): T = other

}

trait NothingType[T <: Type[T]] extends Type[T] {
  self: T =>

  override def subTypeOf(other: T): Boolean = true

  override def union(other: T): T = other

  override def canIntersect(other: T): Boolean = true

  override def intersect(other: T): T = this

}

trait UnionType[T <: Type[T]] extends Type[T] {
  self: T =>

  def ors: Set[T]

  override def canIntersect(other: T): Boolean = ors.forall(or => or.canIntersect(other) || other.canIntersect(or))

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

  override def union(other: T): T = {
    other match {
      case i: IntersectionType[T] => intersect(ands.intersect(i.ands))
      case _ => super.union(other)
    }
  }

  override def subTypeOf(other: T): Boolean = other match {
    case u: UnionType[T] => u.ors.exists(or => ands.forall(_.subTypeOf(or)))
    case i: IntersectionType[T] => ands.subsetOf(i.ands)
    case _ if ands.exists(and => other.subTypeOf(and) && and.subTypeOf(other)) => true
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
