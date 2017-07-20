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
package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.opencypher.spark_legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.value._
import org.opencypher.spark_legacy.impl.{StdCypherFrame, StdSlot}

object ProductAsMap extends FrameCompanion {

  def apply(input: StdCypherFrame[Product]): StdCypherFrame[Map[String, CypherValue]] = {
    val outputMapping = input.signature.fields.map {
      field => field.sym -> obtain(input.signature.fieldSlot)(field)
    }
    ProductAsMap(input)(outputMapping)
  }

  private final case class ProductAsMap(input: StdCypherFrame[Product])(outputMapping: Seq[(Symbol, StdSlot)])
    extends StdCypherFrame[Map[String, CypherValue]](input.signature) {

    override def execute(implicit context: RuntimeContext): Dataset[Map[String, CypherValue]] = {
      val in = input.run
      val out = in.map(convert(outputMapping))(context.cypherRecordEncoder)
      out
    }
  }

  private final case class convert(slots: Seq[(Symbol, StdSlot)]) extends (Product => Map[String, CypherValue]) {

    def apply(product: Product) = {
      val values = product.productIterator.toSeq
      val builder = Map.newBuilder[String, CypherValue]
      slots.foreach {
        case (sym, StdSlot(_, _, ordinal, BinaryRepresentation)) =>
          builder += sym.name -> values(ordinal).asInstanceOf[CypherValue]
        case (sym, StdSlot(_, _, ordinal, EmbeddedRepresentation(StringType))) =>
          builder += sym.name -> CypherString(values(ordinal).asInstanceOf[String])
        case (sym, StdSlot(_, _, ordinal, EmbeddedRepresentation(BooleanType))) =>
          builder += sym.name -> CypherBoolean(values(ordinal).asInstanceOf[Boolean])
        case (sym, StdSlot(_, _, ordinal, EmbeddedRepresentation(LongType))) =>
          builder += sym.name -> CypherInteger(values(ordinal).asInstanceOf[Long])
        case (sym, StdSlot(_, _, ordinal, EmbeddedRepresentation(DoubleType))) =>
          builder += sym.name -> CypherFloat(values(ordinal).asInstanceOf[Double])
      }
      builder.result()
    }
  }
}
