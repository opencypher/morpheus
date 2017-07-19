/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.impl.util

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.spark_legacy.api.frame.EmbeddedRepresentation
import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark_legacy.impl.StdSlot
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
