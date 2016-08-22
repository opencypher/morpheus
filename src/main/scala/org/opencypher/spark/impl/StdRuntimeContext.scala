package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.CypherRuntimeContext
import org.opencypher.spark.impl.newvalue.CypherValue
import org.opencypher.spark.impl.util.ProductEncoderFactory

class StdRuntimeContext(val session: SparkSession)
  extends CypherRuntimeContext with CypherValue.Encoders {

  def productEncoder(slots: Seq[StdSlot]) =
    ProductEncoderFactory.createEncoder(slots)(session)
}
