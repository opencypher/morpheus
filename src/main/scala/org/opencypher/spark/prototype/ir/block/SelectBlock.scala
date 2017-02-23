package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir._

final case class SelectBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: SelectedFields[E],
  where: Where[E] = Where.everything[E]
) extends BasicBlock[SelectedFields[E], E, SelectBlockType.type] {
  override def blockType = SelectBlockType
}

final case class SelectedFields[E](fields: Set[Field]) extends Binds[E]

case object SelectedFields {
  def none[E] = SelectedFields[E](Set.empty)
}

