package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTRelationship
import org.opencypher.spark.api.{CypherRelationship, CypherValue}
import org.opencypher.spark.impl.{PlanningContext, StdCypherFrame, StdField, StdFrameSignature}

object AllRelationships {

  def apply(fieldSym: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherRelationship] = {
    val field = StdField(fieldSym, CTRelationship)
    new AllRelationships(
      input = context.relationships,
      sig = StdFrameSignature.empty.addField(field)
    )
  }

  private final class AllRelationships(input: Dataset[CypherRelationship], sig: StdFrameSignature)
    extends StdCypherFrame[CypherRelationship](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherRelationship] = {
      alias(input)(context.cypherRelationshipEncoder)
    }

    def relField: StdField = sig.fields.head
  }
}
