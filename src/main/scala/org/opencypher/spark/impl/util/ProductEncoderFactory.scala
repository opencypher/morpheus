package org.opencypher.spark.impl.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.opencypher.spark.CypherTypes.{CTInteger, CTString}
import org.opencypher.spark.impl.StdSlot
import org.opencypher.spark.{BinaryRepresentation, CypherValue, EmbeddedRepresentation, _}

case object ProductEncoderFactory {

  def createEncoder(slots: Seq[StdSlot])(implicit session: SparkSession): ExpressionEncoder[Product] = {
    import session.implicits._

    assert(slots.nonEmpty, throw new IllegalArgumentException("Needs at least one slot"))

    val encoders = slots.toList.map {
      case StdSlot(_, _, _, BinaryRepresentation) => CypherValue.implicits.cypherValueEncoder[CypherValue]
      case StdSlot(_, CTString, _, EmbeddedRepresentation(StringType)) => implicitly[Encoder[String]]
      case StdSlot(_, CTInteger, _, EmbeddedRepresentation(IntegerType)) => implicitly[Encoder[Long]]
      case _ => throw new UnsupportedOperationException("Encoder for additional type combinations needed")
    }.map(_.asInstanceOf[ExpressionEncoder[Any]])
    ExpressionEncoder.tuple(encoders).asInstanceOf[ExpressionEncoder[Product]]
  }

  val SPARK_GENERIC_COLUMN_NAME = "value"

}
