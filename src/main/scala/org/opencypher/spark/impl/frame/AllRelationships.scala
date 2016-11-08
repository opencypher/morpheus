package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTRelationship
import org.opencypher.spark.api.value.CypherRelationship
import org.opencypher.spark.impl.{PlanningContext, RelationshipFrame, StdCypherFrame, StdFrameSignature}

object AllRelationships {

  def apply(relationship: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherRelationship] = {
    val (_, sig) = StdFrameSignature.empty.addField(relationship -> CTRelationship)
    AllRelationships(
      input = context.relationships,
      sig = sig
    )
  }

  private final case class AllRelationships(input: Dataset[CypherRelationship], sig: StdFrameSignature)
    extends RelationshipFrame(sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherRelationship] = input
  }
}
