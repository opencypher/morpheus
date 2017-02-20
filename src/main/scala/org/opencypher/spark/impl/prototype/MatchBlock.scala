package org.opencypher.spark.impl.prototype

case class MatchBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given,
  where: Where[E] = Where.everything,
  blockType: MatchBlockType = StandardMatchBlockType
) extends BasicBlockDef[E]

case class ProjectBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  given: Given = Given.nothing,
  where: Where[E],
  yields: Yields[E],
  blockType: BlockType = ProjectBlockType
) extends BasicBlockDef[E]

case class ReturnBlock[E](
  after: Set[BlockRef],
  over: BlockSignature,
  blockType: BlockType = ReturnBlockType
) extends BlockDef[E]
