
package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir._

final case class ProjectBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: ProjectedFields[E] = ProjectedFields.none,
  where: Where[E] = Where.everything
) extends BasicBlock[ProjectedFields[E], E, ProjectBlockType.type] {
  override def blockType = ProjectBlockType
}

final case class ProjectedFields[E](items: Map[Field, E]) extends Binds[E] {
  override def fields = items.keySet
}

case object ProjectedFields {
  def none[E] = ProjectedFields[E](Map.empty)
}

