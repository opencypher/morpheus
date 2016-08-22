package org.opencypher.spark.impl.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.spark.api.frame.EmbeddedRepresentation
import org.opencypher.spark.api.frame.BinaryRepresentation
import org.opencypher.spark.impl.StdSlot
import org.opencypher.spark.api.value.CypherValue

object ProductEncoderFactory {

  import org.opencypher.spark.api.value.CypherValue.Encoders._

  def createEncoder(slots: Seq[StdSlot])(implicit session: SparkSession): ExpressionEncoder[Product] = {
    import session.implicits._

    assert(slots.nonEmpty, throw new IllegalArgumentException("Needs at least one slot"))

    val encoders: List[ExpressionEncoder[_]] = slots.toList.map {

      case StdSlot(_, _, _, BinaryRepresentation) =>
        encoder[CypherValue]

      case StdSlot(_, CTString, _, EmbeddedRepresentation(StringType)) =>
        encoder[String]

      case StdSlot(_, CTString.nullable, _, EmbeddedRepresentation(StringType)) =>
        encoder[String]

      case StdSlot(_, CTInteger, _, EmbeddedRepresentation(LongType)) =>
        encoder[Long]

      case StdSlot(_, CTInteger.nullable, _, EmbeddedRepresentation(LongType)) =>
        encoder[Long]

      case StdSlot(_, CTBoolean, _, EmbeddedRepresentation(BooleanType)) =>
        encoder[Boolean]

      case StdSlot(_, CTBoolean.nullable, _, EmbeddedRepresentation(BooleanType)) =>
        encoder[Boolean]

      case _ =>
        throw new UnsupportedOperationException("Encoder for additional type combinations needed")
    }
    ExpressionEncoder.tuple(encoders).asInstanceOf[ExpressionEncoder[Product]]
  }

  /**
    * This function works by fetching encoders and encoder converters from the implicit scope,
    * and then casts them to ExpressionEncoders via Encoders.asExpressionEncoder.
    * @tparam T the type to encode
    * @return an ExpressionEncoder for the requested type
    */
  private def encoder[T : Encoder]: ExpressionEncoder[T] = implicitly[Encoder[T]]
}
