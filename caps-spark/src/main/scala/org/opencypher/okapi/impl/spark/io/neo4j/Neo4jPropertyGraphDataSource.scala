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
package org.opencypher.okapi.impl.spark.io.neo4j

import org.opencypher.okapi.api.CAPSSession
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.spark.io.CAPSPropertyGraphDataSource
import org.opencypher.okapi.impl.spark.io.neo4j.external.Neo4jConfig

class Neo4jPropertyGraphDataSource(
  config: Neo4jConfig,
  queries: Map[GraphName, (String, String)])(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = queries.get(name) match {
    case Some((nodeQuery, relQuery)) => Neo4jGraphLoader.fromNeo4j(config, nodeQuery, relQuery)
    case None => throw IllegalArgumentException(s"Neo4j graph with name '$name'")
  }

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = queries.keySet

  override def hasGraph(name: GraphName): Boolean = queries.contains(name)
}
