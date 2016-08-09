package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.CypherRuntimeContext
import org.opencypher.spark.impl.util.ProductEncoderFactory

class StdRuntimeContext(val session: SparkSession) extends CypherRuntimeContext {

  def productEncoder(slots: Seq[StdSlot]) =
    ProductEncoderFactory.createEncoder(slots)(session)
}
