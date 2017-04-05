package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark.api.value.CypherNode

object AllNodes extends FrameCompanion {

  def apply(fieldSym: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherNode] = {
    val (_, sig) = StdFrameSignature.empty.addField(fieldSym -> CTNode)
    CypherNodes(
      input = context.nodes,
      sig = sig
    )
  }

  private final case class CypherNodes(input: Dataset[CypherNode], sig: StdFrameSignature)
    extends NodeFrame(sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherNode] = input
  }
}
