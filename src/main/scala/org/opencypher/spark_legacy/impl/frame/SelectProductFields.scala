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
import org.opencypher.spark_legacy.impl._

object SelectProductFields {

  def apply(input: StdCypherFrame[Product])(fields: Symbol*): StdCypherFrame[Product] = {
    val (slotMapping, newSignature) = input.signature.selectFields(fields: _*)
    SelectProductFields(input)(newSignature, slotMapping)
  }

  private final case class SelectProductFields(input: StdCypherFrame[Product])(sig: StdFrameSignature, slots: Seq[StdSlot])
    extends ProductFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val out = input.run.map(selectFields(slots))(context.productEncoder(sig.slots))
      out
    }
  }

  private final case class selectFields(slots: Seq[StdSlot]) extends (Product => Product) {

    import org.opencypher.spark_legacy.impl.util._

    def apply(product: Product): Product = {
      val builder = Vector.newBuilder[Any]
      builder.sizeHint(slots.size)
      slots.foreach { slot => builder += product.get(slot.ordinal) }
      val newValue = builder.result()
      newValue.asProduct
    }
  }
}
