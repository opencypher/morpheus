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
package org.opencypher.spark.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.graph.GraphSpace
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.impl.instances.spark.SparkCypherInstances
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.opencypher.spark.impl.spark.{SparkCypherEngine, SparkGraphLoading}

trait SparkGraphSpace extends GraphSpace {

  self =>

  override type Engine = SparkCypherEngine

  // TODO: Remove
  def tokens: SparkCypherRecordsTokens

  def session: SparkSession
  val engine = org.opencypher.spark.impl.instances.spark.cypher.sparkCypherEngineInstance
}

object SparkGraphSpace extends SparkGraphLoading with Serializable {
  def empty(sparkSession: SparkSession, registry: TokenRegistry) = new SparkGraphSpace {
    override def session: SparkSession = sparkSession
    override def tokens: SparkCypherRecordsTokens = SparkCypherRecordsTokens(registry)
    override def base: SparkCypherGraph = SparkCypherGraph.empty(this)
  }
}
