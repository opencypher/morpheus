package org.opencypher.spark.api.ir.block
import org.opencypher.spark.api.ir.pattern.AllGiven

final case class OrderAndSliceBlock[E](
    after: Set[BlockRef],
    binds: OrderedFields[E],
    orderBy: Seq[SortItem[E]],
    graph: BlockRef,
    skip: Option[E],
    limit: Option[E] )
extends BasicBlock[OrderedFields[E], E](BlockType("order-and-slice")) {

  override def where: AllGiven[E] = AllGiven(Set())
}

sealed trait SortItem[E] {
  def expr: E
}

final case class Asc[E](expr: E) extends SortItem[E]
final case class Desc[E](expr: E) extends SortItem[E]
