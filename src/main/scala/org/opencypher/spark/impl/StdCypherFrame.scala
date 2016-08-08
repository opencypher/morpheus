package org.opencypher.spark.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark._

abstract class StdCypherFrame[Out](val fields: Seq[StdField], val slots: Seq[StdSlot]) extends CypherFrame[Out] {

  override type Frame = StdCypherFrame[Out]
  override type FrameContext = StdFrameContext
  override type Field = StdField
  override type Slot = StdSlot

  def fieldSlots = fields.zip(slots)

  protected def productEncoder(implicit context: FrameContext) =
    ProductEncoderFactory.createEncoder(slots)(context.session)
}


case class StdFrameContext(session: SparkSession) extends CypherFrameContext

case class StdField(name: Symbol, cypherType: CypherType) extends CypherField with Serializable

case class StdSlot(name: Symbol, cypherType: CypherType, representation: Representation) extends CypherSlot with Serializable
