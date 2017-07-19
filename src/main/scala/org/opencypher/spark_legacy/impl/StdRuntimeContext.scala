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
package org.opencypher.spark_legacy.impl

import org.apache.spark.sql.SparkSession
import org.opencypher.spark_legacy.api.frame.CypherRuntimeContext
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark_legacy.impl.util.ProductEncoderFactory
import org.opencypher.spark.api.expr.Const
import org.opencypher.spark.api.ir.global.GlobalsRegistry

class StdRuntimeContext(val session: SparkSession, val parameters: Map[String, CypherValue], val globals: GlobalsRegistry = null)
  extends CypherRuntimeContext with CypherValue.Encoders {

  def productEncoder(slots: Seq[StdSlot]) =
    ProductEncoderFactory.createEncoder(slots)(session)

  def paramValue(p: Const): CypherValue = {
    parameters(p.constant.name)
  }
}
