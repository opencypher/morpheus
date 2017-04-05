
package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.pattern.AllGiven

final case class ProjectBlock[E](
  after: Set[BlockRef],
  binds: ProjectedFields[E] = ProjectedFields[E](),
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[ProjectedFields[E], E](BlockType("project"))

final case class ProjectedFields[E](items: Map[Field, E] = Map.empty[Field, E]) extends Binds[E] {
  override def fields = items.keySet
}

case object ProjectedFieldsOf {
  def apply[E](entries: (Field, E)*) = ProjectedFields(entries.toMap)
}
