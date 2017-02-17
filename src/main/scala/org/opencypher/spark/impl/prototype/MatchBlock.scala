package org.opencypher.spark.impl.prototype

case class MatchBlock(
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given,
  where: Where = Where.everything,
  blockType: MatchBlockType = StandardMatchBlockType
) extends BasicBlockDef

case class ProjectBlock(
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given = Given.nothing,
  where: Where,
  yields: Yields,
  blockType: BlockType = ProjectBlockType
) extends BasicBlockDef

case class ReturnBlock(
  after: Set[BlockRef],
  over: BlockSignature,
  blockType: BlockType = ReturnBlockType
) extends BlockDef
