package org.opencypher.spark.impl.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StringType
import org.opencypher.spark.CypherTypes.CTString
import org.opencypher.spark.impl.StdSlot
import org.opencypher.spark.{BinaryRepresentation, CypherValue, EmbeddedRepresentation}

case object ProductEncoderFactory {

  def createEncoder(slots: Seq[StdSlot])(implicit session: SparkSession): ExpressionEncoder[Product] = {
    val encoders = slots.map {
      case StdSlot(_, _, BinaryRepresentation) => CypherValue.implicits.cypherValueEncoder[CypherValue]
      case StdSlot(_, CTString, EmbeddedRepresentation(StringType)) => session.implicits.newStringEncoder
      case _ => throw new UnsupportedOperationException("Map more types")
    }.map(_.asInstanceOf[ExpressionEncoder[Any]])
    ExpressionEncoder.tuple(encoders).asInstanceOf[ExpressionEncoder[Product]]
  }
}
