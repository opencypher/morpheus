package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir.pattern.{AllGiven, Pattern}

final case class MatchBlock[E](
  after: Set[BlockRef],
  binds: Pattern[E],
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[Pattern[E], E](BlockType("match"))
