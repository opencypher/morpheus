/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.caps.api.graph.CypherResult
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.physical.CAPSQueryPlans
import org.opencypher.caps.impl.util.PrintOptions

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

trait CAPSResult extends CypherResult {

  type Graph = CAPSGraph

  override def records: CAPSRecords

  override def graphs: Map[String, Graph]

  def as[E <: Product : TypeTag]: Iterator[E] = {
    implicit val encoder = ExpressionEncoder[E]
    records.asCaps.data.as[E].toLocalIterator().asScala
  }

  override def show(implicit options: PrintOptions): Unit =
    records.show

  override def plans: CAPSQueryPlans

  override def toString = this.getClass.getSimpleName
}
