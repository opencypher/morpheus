package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.pattern.AllGiven

case class LoadGraphBlock[E](
  after: Set[BlockRef],
  binds: GraphDescriptor[E]
) extends BasicBlock[GraphDescriptor[E], E](BlockType("load-graph")) {
  override def where: AllGiven[E] = AllGiven[E]()

  override def graph: BlockRef = ???
}

sealed trait GraphDescriptor[E] extends Binds[E] {
  override def fields: Set[Field] = Set.empty
}

final case class DefaultGraph[E]() extends GraphDescriptor[E]
