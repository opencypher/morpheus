package org.opencypher.spark.prototype.impl.ir

import java.util.concurrent.atomic.AtomicLong

import org.opencypher.spark.prototype.api.ir.block.{Block, BlockRef, BlockType}

object BlockRegistry {
  def empty[E] = BlockRegistry[E](Seq.empty)
}

// TODO: Make this inherit from Register
case class BlockRegistry[E](reg: Seq[(BlockRef, Block[E])]) {

  def register(blockDef: Block[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)
  }

  // TODO: Add name generation to monads working with this?
  val c = new AtomicLong()

  private def generateName(t: BlockType) = s"${t.name}_${c.incrementAndGet()}"
}
