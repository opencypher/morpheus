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
package org.opencypher.caps.api.io

import org.opencypher.caps.api.graph.{GraphName, PropertyGraph}
import org.opencypher.caps.api.schema.Schema

/**
  * The Property Graph Data Source (PGDS) is used to read and write property graphs from and to an external storage
  * (e.g., a database system, a file system or an memory-based collections of graph data).
  *
  * The PGDS is the main interface for implementing custom data sources for specific openCypher implementations
  * (e.g., for Apache Spark, etc.).
  *
  * The (PGDS) is able to handle multiple property graphs and  distinguishes between them using [[GraphName]]s.
  * Furthermore, a PGDS can be registered at a [[org.opencypher.caps.api.graph.CypherSession]] using a specific
  * [[org.opencypher.caps.api.graph.Namespace]] which enables accessing a [[PropertyGraph]] within a Cypher query.
  */
trait PropertyGraphDataSource {

  /**
    * Returns {{true}} if the data source stores a graph under the given [[GraphName]].
    *
    * @param name name of the graph within the data source
    * @return {{true}}, iff the graph is stored within the data source
    */
  def hasGraph(name: GraphName): Boolean

  /**
    * Returns the [[PropertyGraph]] that is stored under the given name.
    *
    * @param name name of the graph within the data source
    * @return property graph
    */
  def graph(name: GraphName): PropertyGraph

  /**
    * Returns the [[Schema]] of the graph that is stored under the given name.
    *
    * This method gives implementers the ability to efficiently retrieve a graph schema from the data source directly.
    * For reasons of performance, it is highly recommended to make a schema available through this call. If an efficient
    * retrieval is not possible, the call is typically forwarded to the graph using the [[PropertyGraph.schema]] call.
    *
    * @param name name of the graph within the data source
    * @return graph schema
    */
  def schema(name: GraphName): Option[Schema]

  /**
    * Stores the given [[PropertyGraph]] under the given [[GraphName]] within the data source.
    *
    * @param name  name under which the graph shall be stored
    * @param graph property graph
    */
  def store(name: GraphName, graph: PropertyGraph): Unit

  /**
    * Deletes the [[PropertyGraph]] within the data source that is stored under the given [[GraphName]].
    *
    * @param name name under which the graph is stored
    */
  def delete(name: GraphName): Unit

  /**
    * Returns the [[GraphName]]s of all [[PropertyGraph]]s stored within the data source.
    *
    * @return names of stored graphs
    */
  def graphNames: Set[GraphName]

}
