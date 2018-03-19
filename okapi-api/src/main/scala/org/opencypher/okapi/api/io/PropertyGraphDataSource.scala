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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.api.io

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema

/**
  * The Property Graph Data Source (PGDS) is used to read and write property graphs from and to an external storage
  * (e.g., a database system, a file system or an memory-based collections of graph data).
  *
  * The PGDS is the main interface for implementing custom data sources for specific openCypher implementations
  * (e.g., for Apache Spark, etc.).
  *
  * The (PGDS) is able to handle multiple property graphs and  distinguishes between them using [[org.opencypher.okapi.api.graph.GraphName]]s.
  * Furthermore, a PGDS can be registered at a [[org.opencypher.okapi.api.graph.CypherSession]] using a specific
  * [[org.opencypher.okapi.api.graph.Namespace]] which enables accessing a [[org.opencypher.okapi.api.graph.PropertyGraph]] within a Cypher query.
  */
trait PropertyGraphDataSource {

  /**
    * Returns `true` if the data source stores a graph under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name of the graph within the data source
    * @return `true`, iff the graph is stored within the data source
    */
  def hasGraph(name: GraphName): Boolean

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] that is stored under the given name.
    *
    * @param name name of the graph within the data source
    * @return property graph
    */
  def graph(name: GraphName): PropertyGraph

  /**
    * Returns the [[org.opencypher.okapi.api.schema.Schema]] of the graph that is stored under the given name.
    *
    * This method gives implementers the ability to efficiently retrieve a graph schema from the data source directly.
    * For reasons of performance, it is highly recommended to make a schema available through this call. If an efficient
    * retrieval is not possible, the call is typically forwarded to the graph using the [[org.opencypher.okapi.api.graph.PropertyGraph#schema]] call, which may require materialising the full graph.
    *
    * @param name name of the graph within the data source
    * @return graph schema
    */
  def schema(name: GraphName): Option[Schema]

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] under the given [[org.opencypher.okapi.api.graph.GraphName]] within the data source.
    *
    * @param name  name under which the graph shall be stored
    * @param graph property graph
    */
  def store(name: GraphName, graph: PropertyGraph): Unit

  /**
    * Deletes the [[org.opencypher.okapi.api.graph.PropertyGraph]] within the data source that is stored under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name under which the graph is stored
    */
  def delete(name: GraphName): Unit

  /**
    * Returns the [[org.opencypher.okapi.api.graph.GraphName]]s of all [[org.opencypher.okapi.api.graph.PropertyGraph]]s stored within the data source.
    *
    * @return names of stored graphs
    */
  def graphNames: Set[GraphName]

}
