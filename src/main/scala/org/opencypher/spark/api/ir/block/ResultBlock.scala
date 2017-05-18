package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.pattern.{AllGiven, AllOf}

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  nodes: Set[Field],
  relationships: Set[Field],
  graph: BlockRef,
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFields[E], E](BlockType("result")) {

  def select(fields: Set[Field]): ResultBlock[E] =
    copy(binds = binds.select(fields), nodes = nodes intersect fields, relationships = relationships intersect fields )
}

object ResultBlock {
  def empty[E](graphBlock: BlockRef) = ResultBlock(Set.empty, OrderedFields[E](), Set.empty, Set.empty, graphBlock, AllOf[E]())
}

final case class OrderedFields[E](fieldsOrder: Seq[Field] = Seq.empty) extends Binds[E] {
  override def fields = fieldsOrder.toSet

  def select(fields: Set[Field]): OrderedFields[E] =
    copy(fieldsOrder = fieldsOrder.filter(fields.contains))
}

case object FieldsInOrder {
  def apply[E](fields: Field*) = OrderedFields[E](fields)
  def unapplySeq(arg: OrderedFields[_]): Option[Seq[Field]] = Some(arg.fieldsOrder)
}

