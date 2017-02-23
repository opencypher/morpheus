package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir.pattern.Pattern

final case class MatchBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: Pattern[E],
  where: Where[E] = Where.everything,
  blockType: MatchBlockType = StandardMatchBlockType
) extends BasicBlock[Pattern[E], E, MatchBlockType]
