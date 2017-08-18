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
package org.opencypher.caps.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.graph.GraphSpace
import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.impl.instances.spark.CAPSInstances
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.{CAPSEngine, SparkGraphLoading}

trait SparkGraphSpace extends GraphSpace {

  self =>

  override type Engine = CAPSEngine

  // TODO: Remove
  var tokens: CAPSRecordsTokens

  def session: SparkSession
  val engine = org.opencypher.caps.impl.instances.spark.cypher.sparkCypherEngineInstance
}

object SparkGraphSpace extends SparkGraphLoading with Serializable {
  def empty(sparkSession: SparkSession, registry: TokenRegistry) = new SparkGraphSpace {
    override def session: SparkSession = sparkSession
    override var tokens: CAPSRecordsTokens = CAPSRecordsTokens(registry)
    override def base: CAPSGraph = CAPSGraph.empty(this)
  }
}
