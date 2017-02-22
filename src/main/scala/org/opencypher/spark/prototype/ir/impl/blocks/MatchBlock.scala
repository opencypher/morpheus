package org.opencypher.spark.prototype.ir.impl.blocks

import org.opencypher.spark.prototype.ir._

case class MatchBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given,
  where: Where[E] = Where.everything,
  blockType: MatchBlockType = StandardMatchBlockType
) extends BasicBlockDef[E]
