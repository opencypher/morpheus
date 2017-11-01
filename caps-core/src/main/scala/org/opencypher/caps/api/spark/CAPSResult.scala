/*
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
package org.opencypher.caps.api.spark

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.caps.api.graph.CypherResult
import org.opencypher.caps.api.util.PrintOptions

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.TypeTag

trait CAPSResult extends CypherResult {

  override type Graph   = CAPSGraph
  override type Records = CAPSRecords

  def as[E <: Product: TypeTag]: Iterator[E] = {
    implicit val encoder = ExpressionEncoder[E]
    records.data.as[E].toLocalIterator()
  }

  override def print(implicit options: PrintOptions): Unit =
    records.print

  override def toString = this.getClass.getSimpleName
}
