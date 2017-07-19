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

import org.opencypher.spark_legacy.impl.StdFrameSignature
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.{CypherInteger, CypherList, CypherValue}

sealed trait AggregationFunction {
  def inField: Symbol
  def outField: Symbol
  def outType(sig: StdFrameSignature): CypherType

  def unit: CypherValue
}

case class Count(inField: Symbol)(val outField: Symbol) extends AggregationFunction {
  override def outType(sig: StdFrameSignature): CypherType = CTInteger
  override def unit = CypherInteger(0)
}

case class Collect(inField: Symbol)(val outField: Symbol) extends AggregationFunction {
  override def outType(sig: StdFrameSignature): CypherType = CTList(sig.field(inField).get.cypherType)
  override def unit = CypherList.empty
}

