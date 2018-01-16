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

import cats.Eq

import scala.util.hashing.MurmurHash3

sealed trait Orientation[E <: Endpoints] extends Eq[E] {
  def hash(ends: E, seed: Int): Int
}

object Orientation {
  case object Directed extends Orientation[DifferentEndpoints] {
    override def hash(ends: DifferentEndpoints, seed: Int) = MurmurHash3.orderedHash(ends, seed)
    override def eqv(x: DifferentEndpoints, y: DifferentEndpoints) = x.source == y.source && x.target == y.target
  }

  case object Undirected extends Orientation[DifferentEndpoints] {
    override def hash(ends: DifferentEndpoints, seed: Int) = MurmurHash3.unorderedHash(ends, seed)
    override def eqv(x: DifferentEndpoints, y: DifferentEndpoints) =
      (x.source == y.source && x.target == y.target) || (x.source == y.target && x.target == y.source)
  }

  case object Cyclic extends Orientation[IdenticalEndpoints] {
    override def hash(ends: IdenticalEndpoints, seed: Int) = MurmurHash3.mix(seed, ends.field.hashCode())
    override def eqv(x: IdenticalEndpoints, y: IdenticalEndpoints) = x.field == y.field
  }
}
