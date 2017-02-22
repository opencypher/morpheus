package org.opencypher.spark.prototype.ir.impl.blocks

import org.opencypher.spark.prototype.ir._

case class ReturnBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  blockType: BlockType = ReturnBlockType
) extends BlockDef[E]
