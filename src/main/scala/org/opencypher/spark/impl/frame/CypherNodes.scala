package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.{CypherTypes, BinaryRepresentation, CypherNode}
import CypherTypes.CTNode
import org.opencypher.spark.impl.{StdCypherFrame, StdField, PlanningContext, StdSlot}
import org.opencypher.spark.{CypherTypes, BinaryRepresentation, CypherNode}

object CypherNodes {
  def apply(input: Dataset[CypherNode])(fieldSym: Symbol)(implicit context: PlanningContext): CypherNodes = {
    val field = StdField(fieldSym, CTNode)
    new CypherNodes(
      input = input,
      sig = StdFrameSignature.empty.addField(field)
    )
  }

  class CypherNodes(input: Dataset[CypherNode], sig: StdFrameSignature) extends StdCypherFrame[CypherNode](sig) {

    override def run(implicit context: RuntimeContext): Dataset[CypherNode] = {
      input
    }

    def nodeField: StdField = sig.fields.head
  }
}
