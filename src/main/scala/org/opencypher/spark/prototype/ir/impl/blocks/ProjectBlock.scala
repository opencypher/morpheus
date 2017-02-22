package org.opencypher.spark.prototype.ir.impl.blocks

import org.opencypher.spark.prototype.ir._

case class ProjectBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given = Given.nothing,
  where: Where[E],
  yields: Yields[E],
  blockType: BlockType = ProjectBlockType
) extends BasicBlockDef[E]
