package org.opencypher.spark.impl.prototype

case class MatchBlock(
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given,
  where: Where = Where.everything,
  blockType: MatchBlockType = StandardMatchBlockType
) extends BasicBlockDef
