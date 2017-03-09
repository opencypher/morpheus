package org.opencypher.spark.prototype.api.ir.block

import org.opencypher.spark.prototype.api.ir.Field

import scala.language.implicitConversions

final case class BlockSig(inputs: Set[Field], outputs: Set[Field])

case object BlockSig {
  val empty = BlockSig(Set.empty, Set.empty)

  implicit def signature(pairs: (Set[Field], Set[Field])): BlockSig = BlockSig(pairs._1, pairs._2)
}
