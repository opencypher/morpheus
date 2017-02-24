package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.pattern.AllGiven

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFields[E], E](BlockType("result"))

final case class OrderedFields[E](fieldsOrder: Seq[Field] = Seq.empty) extends Binds[E] {
  override def fields = fieldsOrder.toSet
}

case object FieldsInOrder {
  def apply(fields: Field*) = OrderedFields(fields)
  def unapplySeq(arg: OrderedFields[_]): Option[Seq[Field]] = Some(arg.fieldsOrder)
}

