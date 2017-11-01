/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.ir.api.pattern

// TODO: AnyVal
trait Elements[T] {
  def isEmpty: Boolean = elements.isEmpty
  def headOption: Option[T]
  def tail: Elements[T]

  def flatPartition[U](f: PartialFunction[T, U]): (Elements[U], Elements[T])
  def partition(f: T => Boolean): (Elements[T], Elements[T])
  def filter(f: T => Boolean): Elements[T]
  def filterNot(f: T => Boolean): Elements[T]
  def map[X](f: T => X): Elements[X]
  def elements: Set[T]
}

final case class AnyGiven[T](elements: Set[T] = Set.empty[T]) extends Elements[T] {
  override def headOption: Option[T] = elements.headOption
  override def tail: AnyGiven[T]     = AnyGiven(elements.tail)

  def flatPartition[U](f: PartialFunction[T, U]): (AnyGiven[U], AnyGiven[T]) = {
    val (selected, remaining) = elements.partition(f.isDefinedAt)
    AnyGiven(selected.map(f)) -> AnyGiven(remaining)
  }

  override def partition(f: T => Boolean): (AnyGiven[T], AnyGiven[T]) = {
    val (left, right) = elements.partition(f)
    AnyGiven(left) -> AnyGiven(right)
  }
  override def filter(f: T => Boolean): AnyGiven[T]    = AnyGiven(elements.filter(f))
  override def filterNot(f: T => Boolean): AnyGiven[T] = AnyGiven(elements.filterNot(f))
  override def map[X](f: T => X): AnyGiven[X]          = AnyGiven(elements.map(f))
}

case object AnyOf {
  def apply[T](elts: T*) = AnyGiven(elts.toSet)
}

final case class AllGiven[T](elements: Set[T] = Set.empty[T]) extends Elements[T] {
  override def headOption: Option[T] = elements.headOption
  override def tail: AnyGiven[T]     = AnyGiven(elements.tail)

  def flatPartition[U](f: PartialFunction[T, U]): (AllGiven[U], AllGiven[T]) = {
    val (selected, remaining) = elements.partition(f.isDefinedAt)
    AllGiven(selected.map(f)) -> AllGiven(remaining)
  }

  override def partition(f: T => Boolean): (AllGiven[T], AllGiven[T]) = {
    val (left, right) = elements.partition(f)
    AllGiven(left) -> AllGiven(right)
  }
  override def filter(f: T => Boolean): AllGiven[T]    = AllGiven(elements.filter(f))
  override def filterNot(f: T => Boolean): AllGiven[T] = AllGiven(elements.filterNot(f))
  override def map[X](f: T => X)                       = AllGiven(elements.map(f))
}

case object AllOf {
  def apply[T](elts: T*) = AllGiven(elts.toSet)
}
