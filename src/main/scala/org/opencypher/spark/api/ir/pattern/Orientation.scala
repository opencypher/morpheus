package org.opencypher.spark.api.ir.pattern

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
