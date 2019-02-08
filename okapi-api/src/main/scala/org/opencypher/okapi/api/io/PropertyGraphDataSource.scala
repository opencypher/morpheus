/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}

/**
  * Property Graph Data Source (PGDS) is used to read and write property graphs, for example from database or
  * file systems, memory-based collections, etc.
  *
  * [[PropertyGraphDataSource]] is the main interface for connecting custom data sources for specific openCypher implementations.
  *
  * A PGDS can handle multiple property graphs and distinguishes between them using [[org.opencypher.okapi.api.graph.GraphName]]s.
  * Furthermore, a PGDS can be registered at a [[org.opencypher.okapi.api.graph.CypherSession]] using a specific
  * [[org.opencypher.okapi.api.graph.Namespace]] which enables accessing a [[org.opencypher.okapi.api.graph.PropertyGraph]] from within a Cypher query.
  */
trait PropertyGraphDataSource {

  /**
    * Returns `true` if the data source can provide a graph for the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name of the graph within the data source
    * @return `true`, iff the graph can be provided
    */
  def hasGraph(name: GraphName): Boolean

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] for the given name.
    *
    * Throws a [[org.opencypher.okapi.impl.exception.GraphNotFoundException]] when that graph cannot be provided.
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
    * retrieval is not possible, the call is typically forwarded to the graph using the [[org.opencypher.okapi.api.graph.PropertyGraph#schema]]
    * call, which may require materialising the full graph.
    *
    * Returns `None` when the schema cannot be provided.
    *
    * @param name name of the graph within the data source
    * @return graph schema when available
    */
  def schema(name: GraphName): Option[Schema]

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] under the given [[org.opencypher.okapi.api.graph.GraphName]] within the data source.
    *
    * If the data source already stores a graph under the given name, a [[org.opencypher.okapi.impl.exception.GraphAlreadyExistsException]] should be thrown.
    *
    * Throws an [[java.lang.UnsupportedOperationException]] if not supported.
    *
    * @param name  name under which the graph shall be stored
    * @param graph property graph to store
    */
  def store(name: GraphName, graph: PropertyGraph): Unit

  /**
    * Deletes the [[org.opencypher.okapi.api.graph.PropertyGraph]] within the data source that is stored under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * Throws an [[java.lang.UnsupportedOperationException]] if not supported.
    *
    * This operation will do nothing if the graph is not found.
    *
    * @param name name under which the graph is stored
    */
  def delete(name: GraphName): Unit

  /**
    * Returns a set of [[org.opencypher.okapi.api.graph.GraphName]]s for [[org.opencypher.okapi.api.graph.PropertyGraph]]s
    * that can be provided by this data source.
    *
    * For data sources that provide a known set of graphs, this returns the set of all graphs that can be provided.
    *
    * For data sources that can construct graphs dynamically, this merely returns the names of the graphs that have
    * already been provided and not yet deleted.
    *
    * For every returned name, `hasGraph` is guaranteed to return `true`.
    *
    * @return names of graphs that can be provided
    */
  def graphNames: Set[GraphName]

}

trait PatternProvider {

  self: PropertyGraphDataSource =>

  def patterns(graph: GraphName): Seq[NodeRelPattern]
}

case class NodeRelPattern(node: CTNode, rel: CTRelationship)
