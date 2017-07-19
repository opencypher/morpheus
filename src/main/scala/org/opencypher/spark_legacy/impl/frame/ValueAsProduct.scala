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
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark_legacy.impl.{ProductFrame, StdCypherFrame, StdRuntimeContext}

object ValueAsProduct extends FrameCompanion {

  def apply[T <: CypherValue](input: StdCypherFrame[T]): ProductFrame =
    ValueAsProduct(input)

  private final case class ValueAsProduct[T <: CypherValue](input: StdCypherFrame[T]) extends ProductFrame(input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(convert)(context.productEncoder(slots))
      out
    }
  }

  private case object convert extends (CypherValue => Product) {
    override def apply(v: CypherValue): Product = Tuple1(v)
  }
}
