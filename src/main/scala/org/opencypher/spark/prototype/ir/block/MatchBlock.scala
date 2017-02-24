package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir.pattern.{AllGiven, Pattern}

final case class MatchBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: Pattern[E],
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[Pattern[E], E](BlockType("match"))
