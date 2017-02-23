package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.token.TokenRegistry

final case class QueryModel[E](
  result: ResultBlock[E],
  tokens: TokenRegistry,
  blocks: Map[BlockRef, Block[E]]
) extends (BlockRef => Block[E]) {

  override def apply(ref: BlockRef): Block[E] = blocks(ref)
}
