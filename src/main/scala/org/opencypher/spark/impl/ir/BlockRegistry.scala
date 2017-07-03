package org.opencypher.spark.impl.ir

import org.opencypher.spark.api.ir.block.{Block, BlockRef, BlockType}

object BlockRegistry {
  def empty[E] = BlockRegistry[E](Seq.empty)()
}

// TODO: Make this inherit from Register
case class BlockRegistry[E](reg: Seq[(BlockRef, Block[E])])(private val counter: Int = 0) {

  def register(blockDef: Block[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)(counter + 1)
  }

  def apply(ref: BlockRef): Block[E] = reg.find {
    case (_ref, b) => ref == _ref
  }.getOrElse(throw new NoSuchElementException(s"Didn't find block with reference $ref"))._2

  def lastAdded: Option[BlockRef] = reg.lastOption.map(_._1)

  private def generateName(t: BlockType) = s"${t.name}_$counter"
}
