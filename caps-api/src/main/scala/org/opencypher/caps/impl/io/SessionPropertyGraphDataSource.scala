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
package org.opencypher.caps.impl.io

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{GraphName, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema

object SessionPropertyGraphDataSource {

  val Namespace = org.opencypher.caps.api.io.Namespace("session")
}

class SessionPropertyGraphDataSource() extends PropertyGraphDataSource {

  private var graphMap: Map[GraphName, PropertyGraph] = Map.empty

  override def graph(name: GraphName): PropertyGraph = graphMap(name)

  override def schema(name: GraphName): Option[Schema] = Some(graphMap(name).schema)

  override def store(name: GraphName, graph: PropertyGraph): Unit = graphMap = graphMap.updated(name, graph)

  // TODO: uncache graph
  override def delete(name: GraphName): Unit = graphMap = graphMap.filterKeys(_ != name)

  override def graphNames: Set[GraphName] = graphMap.keySet

  override def hasGraph(name: GraphName): Boolean = graphMap.contains(name)
}
