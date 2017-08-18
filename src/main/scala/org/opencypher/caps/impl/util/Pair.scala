/**
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
package org.opencypher.caps.impl.util

import scala.util.hashing.MurmurHash3

case object Pair {
  val hashSeed = "Pair".hashCode
}

sealed trait Pair[T] extends Traversable[T] {
  def fst: T
  def snd: T

  def isLoop = fst == snd

  def isOrdered: Boolean
  def isUnordered: Boolean

  def toOrdered: OrderedPair[T]
  def toUnordered: UnorderedPair[T]

  def toTuple: (T, T) = (fst, snd)

  def flip: Pair[T]

  override def foreach[U](f: (T) => U): Unit = {
    f(fst)
    f(snd)
  }
}

final case class OrderedPair[T](fst: T, snd: T) extends Pair[T] {

  override def isOrdered = true
  override def isUnordered = false

  override def toOrdered = this
  override def toUnordered = UnorderedPair(fst, snd)

  override def hashCode(): Int = MurmurHash3.orderedHash(this, Pair.hashSeed)

  override def flip: OrderedPair[T] = copy(snd, fst)
}

final case class UnorderedPair[T](fst: T, snd: T) extends Pair[T] {

  override def isOrdered = false
  override def isUnordered = true

  override def toOrdered = OrderedPair(fst, snd)
  override def toUnordered = this

  override def hashCode(): Int = MurmurHash3.unorderedHash(this, Pair.hashSeed)

  override def equals(obj: scala.Any): Boolean = if (super.equals(obj)) true else obj match {
    case other: UnorderedPair[_] =>
      val otherFst = other.fst
      val otherSnd = other.snd
      (fst == otherFst && snd == otherSnd) || (snd == otherFst && fst == otherSnd)
    case _ =>
      false
  }

  override def flip: UnorderedPair[T] = this
}
