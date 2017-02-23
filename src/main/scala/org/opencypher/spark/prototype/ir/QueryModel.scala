package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.global.GlobalsRegistry

final case class QueryModel[E](
  result: ResultBlock[E],
  globals: GlobalsRegistry,
//  bindings: Map[ConstantRef, ConstantBinding],
  blocks: Map[BlockRef, Block[E]]
) {

  def apply(ref: BlockRef): Block[E] = blocks(ref)

  def find[T](f: PartialFunction[(BlockRef, Block[E]), T]): Option[T] =
    blocks.collectFirst(f)
}

// sealed trait ConstantBinding
// final case class ParameterBinding(name: String)
