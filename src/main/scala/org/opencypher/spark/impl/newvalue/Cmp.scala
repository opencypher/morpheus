package org.opencypher.spark.impl.newvalue

object Cmp {
  object equality extends Cmp[Option[Int]] {
    def material(cmp: Int) = Some(cmp)
    def nullable(cmp: Int) = None
  }

  object equivalence extends Cmp[Some[Int]] {
    def material(cmp: Int) = Some(cmp)
    def nullable(cmp: Int) = Some(cmp)
  }
}

sealed trait Cmp[C <: Option[Int]] {
  def material(cmp: Int): C
  def nullable(cmp: Int): C
}
