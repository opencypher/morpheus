package org.opencypher.spark.prototype.ir

import scala.language.implicitConversions

case object BlockSignature {
  val empty = BlockSignature(Set.empty, Set.empty)
  implicit def signature(pairs: (Set[Field], Set[Field])): BlockSignature = BlockSignature(pairs._1, pairs._2)
}

final case class BlockSignature(inputs: Set[Field], outputs: Set[Field])
