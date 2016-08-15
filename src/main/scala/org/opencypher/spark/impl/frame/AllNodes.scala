package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherNode
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.impl._

object AllNodes {

  def apply(fieldSym: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherNode] = {
    val (_, sig) = StdFrameSignature.empty.addField(fieldSym, CTNode)
    new CypherNodes(
      input = context.nodes,
      sig = sig
    )
  }

  private final class CypherNodes(input: Dataset[CypherNode], sig: StdFrameSignature)
    extends StdCypherFrame[CypherNode](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherNode] = {
      // rename hard-coded column name 'value' to our slot name
      alias(input)(context.cypherNodeEncoder)
    }
  }
}
