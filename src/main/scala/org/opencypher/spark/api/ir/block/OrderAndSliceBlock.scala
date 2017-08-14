package org.opencypher.spark.api.ir.block
import org.opencypher.spark.api.ir.pattern.AllGiven

final case class OrderAndSliceBlock[E](after: Set[BlockRef],
                                       orderBy: Seq[SortItem[E]],
                                       skip: Option[E],
                                       limit: Option[E],
                                       graph: BlockRef)
extends BasicBlock[OrderedFields[E], E](BlockType("order-and-slice")) {
  override val binds = OrderedFields[E]()
  override def where: AllGiven[E] = AllGiven(Set())
}

sealed trait SortItem[E] {
  def expr: E
}

final case class Asc[E](expr: E) extends SortItem[E]
final case class Desc[E](expr: E) extends SortItem[E]
