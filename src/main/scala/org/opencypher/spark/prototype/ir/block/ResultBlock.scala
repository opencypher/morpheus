package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir._

final case class ResultBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: ResultFields[E],
  where: Where[E] = Where.everything[E]
) extends BasicBlock[ResultFields[E], E, ResultBlockType.type] {
  override def blockType = ResultBlockType
}

final case class ResultFields[E](fieldsOrder: Seq[Field]) extends Binds[E] {
  override def fields = fieldsOrder.toSet
}

case object ResultFields {
  def none[E] = ResultFields[E](Seq.empty)
}

