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
import org.opencypher.spark_legacy.impl.{ProductFrame, StdCypherFrame, StdFrameSignature, StdRuntimeContext}

object DropField extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(field: Symbol): StdCypherFrame[Product] =  {
    val sig = input.signature.dropField(field)
    val slot = obtain(input.signature.slot)(field)

    DropField(input)(slot.ordinal)(sig)
  }

  private case class DropField(input: StdCypherFrame[Product])(index: Int)(sig: StdFrameSignature) extends ProductFrame(sig) {
    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      in.map(reduceProduct(index))(context.productEncoder(slots))
    }
  }

  private case class reduceProduct(index: Int) extends (Product => Product) {
    import org.opencypher.spark_legacy.impl.util._

    override def apply(record: Product): Product = {
      record.drop(index)
    }
  }

}
