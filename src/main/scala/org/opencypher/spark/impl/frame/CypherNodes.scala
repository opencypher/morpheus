package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.CypherTypes.CTNode
import org.opencypher.spark.impl.{StdCypherFrame, StdField, PlanningContext, StdSlot}
import org.opencypher.spark.{BinaryRepresentation, CypherNode}

object CypherNodes {
  def apply(input: Dataset[CypherNode])(fieldSym: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherNode] = {
    val field = StdField(fieldSym, CTNode)
    new CypherNodes(
      input = input,
      field = field,
      slotSym = context.newSlotSymbol(field)
    )
  }

  class CypherNodes(input: Dataset[CypherNode], field: StdField, slotSym: Symbol) extends StdCypherFrame[CypherNode](
    _fields = Seq(field),
    _slots = Seq(StdSlot(slotSym, CTNode, BinaryRepresentation))
  ) {

    override def run(implicit context: RuntimeContext): Dataset[CypherNode] = {
      input
    }
  }

}
