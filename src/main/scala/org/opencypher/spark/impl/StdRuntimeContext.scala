package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.frame.CypherRuntimeContext
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.impl.util.ProductEncoderFactory
import org.opencypher.spark.prototype.api.expr.Const
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry

class StdRuntimeContext(val session: SparkSession, val parameters: Map[String, CypherValue], val globals: GlobalsRegistry = null)
  extends CypherRuntimeContext with CypherValue.Encoders {

  def productEncoder(slots: Seq[StdSlot]) =
    ProductEncoderFactory.createEncoder(slots)(session)

  def paramValue(p: Const): CypherValue = {
    parameters(globals.constant(p.ref).name)
  }
}
