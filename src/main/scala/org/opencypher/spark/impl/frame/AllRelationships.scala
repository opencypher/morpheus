package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.CypherTypes.CTRelationship
import org.opencypher.spark.impl.util.ProductEncoderFactory.SPARK_GENERIC_COLUMN_NAME
import org.opencypher.spark.impl.{PlanningContext, StdCypherFrame, StdField, StdFrameSignature}
import org.opencypher.spark.{CypherRelationship, CypherValue}

object AllRelationships {

  def apply(input: Dataset[CypherRelationship])(fieldSym: Symbol)
           (implicit context: PlanningContext): AllRelationships = {
    val field = StdField(fieldSym, CTRelationship)
    new AllRelationships(
      input = input,
      sig = StdFrameSignature.empty.addField(field)
    )
  }

  class AllRelationships(input: Dataset[CypherRelationship], sig: StdFrameSignature)
    extends StdCypherFrame[CypherRelationship](sig) {

    override def run(implicit context: RuntimeContext): Dataset[CypherRelationship] = {
      alias(input, CypherValue.implicits.cypherValueEncoder[CypherRelationship])
    }

    def relField: StdField = sig.fields.head
  }
}
