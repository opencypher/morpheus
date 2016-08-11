package org.opencypher.spark.impl.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.spark.api.{BinaryRepresentation, CypherValue, EmbeddedRepresentation}
import org.opencypher.spark.impl.StdSlot

case object ProductEncoderFactory {

  import CypherValue.Encoders._

  def createEncoder(slots: Seq[StdSlot])(implicit session: SparkSession): ExpressionEncoder[Product] = {
    import session.implicits._

    val encoders: List[ExpressionEncoder[_]] = slots.toList.map {
      case StdSlot(_, _, _, BinaryRepresentation) =>
        encoder[CypherValue]

      case StdSlot(_, CTString, _, EmbeddedRepresentation(StringType)) =>
        encoder[String]

      case StdSlot(_, CTInteger, _, EmbeddedRepresentation(IntegerType)) =>
        encoder[Long]

      case StdSlot(_, CTBoolean, _, EmbeddedRepresentation(BooleanType)) =>
        encoder[Boolean]

      case _ =>
        throw new UnsupportedOperationException("Encoder for additional type combinations needed")
    }
    ExpressionEncoder.tuple(encoders).asInstanceOf[ExpressionEncoder[Product]]
  }

  private def encoder[T : Encoder]: ExpressionEncoder[T] = implicitly[Encoder[T]]
}
