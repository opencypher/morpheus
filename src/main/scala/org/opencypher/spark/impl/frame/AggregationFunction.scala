package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.{CTInteger, CTList}
import org.opencypher.spark.prototype.api.value.{CypherInteger, CypherList, CypherValue}
import org.opencypher.spark.impl.StdFrameSignature

sealed trait AggregationFunction {
  def inField: Symbol
  def outField: Symbol
  def outType(sig: StdFrameSignature): CypherType

  def unit: CypherValue
}

case class Count(inField: Symbol)(val outField: Symbol) extends AggregationFunction {
  override def outType(sig: StdFrameSignature): CypherType = CTInteger
  override def unit = CypherInteger(0)
}

case class Collect(inField: Symbol)(val outField: Symbol) extends AggregationFunction {
  override def outType(sig: StdFrameSignature): CypherType = CTList(sig.field(inField).get.cypherType)
  override def unit = CypherList.empty
}

