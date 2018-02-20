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
package org.opencypher.caps.impl.spark.io.neo4j

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.GraphName
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.spark.io.CAPSPropertyGraphDataSource
import org.opencypher.caps.impl.spark.io.neo4j.external.Neo4jConfig

class Neo4jPropertyGraphDataSource(
  config: Neo4jConfig,
  queries: Option[(String, String)] = None)(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = queries match {
    case Some((nodeQuery, relQuery)) => Neo4jGraphLoader.fromNeo4j(config, nodeQuery, relQuery)
    case None => Neo4jGraphLoader.fromNeo4j(config) // load the whole graph
  }

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
