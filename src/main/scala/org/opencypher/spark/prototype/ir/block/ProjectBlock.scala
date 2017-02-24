
package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.pattern.AllGiven

final case class ProjectBlock[E](
  after: Set[BlockRef],
  over: BlockSig,
  binds: ProjectedFields[E] = ProjectedFields[E](),
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[ProjectedFields[E], E, ProjectBlockType.type] {
  override def blockType = ProjectBlockType
}

final case class ProjectedFields[E](items: Map[Field, E] = Map.empty[Field, E]) extends Binds[E] {
  override def fields = items.keySet
}

case object ProjectedFieldsOf {
  def apply[E](entries: (Field, E)*) = ProjectedFields(entries.toMap)
}
