package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.frame.CypherRuntimeContext
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.prototype.Const
import org.opencypher.spark.impl.util.ProductEncoderFactory

class StdRuntimeContext(val session: SparkSession, val parameters: Map[String, CypherValue])
  extends CypherRuntimeContext with CypherValue.Encoders {

  def productEncoder(slots: Seq[StdSlot]) =
    ProductEncoderFactory.createEncoder(slots)(session)

  def paramValue(p: Const): CypherValue = {
//    parameters(p)
    null
  }
}
