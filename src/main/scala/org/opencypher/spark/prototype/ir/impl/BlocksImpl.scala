package org.opencypher.spark.prototype.ir.impl

import org.opencypher.spark.prototype.ir.{BlockDef, BlockRef, Blocks}

final case class BlocksImpl[E](blocks: Map[BlockRef, BlockDef[E]], solve: BlockRef) extends Blocks[E] {
  override def apply(ref: BlockRef): BlockDef[E] = blocks(ref)
}
