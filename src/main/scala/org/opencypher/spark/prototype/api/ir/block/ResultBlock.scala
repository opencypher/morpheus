package org.opencypher.spark.prototype.api.ir.block

import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, AllOf}

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFields[E], E](BlockType("result"))

object ResultBlock {
  def empty[E] = ResultBlock(Set.empty, OrderedFields[E](), AllOf[E]())
}

final case class OrderedFields[E](fieldsOrder: Seq[Field] = Seq.empty) extends Binds[E] {
  override def fields = fieldsOrder.toSet
}

case object FieldsInOrder {
  def apply[E](fields: Field*) = OrderedFields[E](fields)
  def unapplySeq(arg: OrderedFields[_]): Option[Seq[Field]] = Some(arg.fieldsOrder)
}

