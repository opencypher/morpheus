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
package org.opencypher.caps.api.graph

import org.opencypher.caps.api.io._
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource.{Namespace => SessionNamespace}

/**
  * The Cypher Session is the main API for a Cypher-based application. It manages graphs which can be queried using
  * Cypher. Graphs can be read from / written to different data sources (e.g. CSV) and also stored in / retrieved from
  * the session-local storage.
  */
trait CypherSession {

  /**
    * The [[org.opencypher.caps.api.graph.Namespace]] used to to store graphs within this session.
    *
    * @return session namespace
    */
  def sessionNamespace: Namespace

  /**
    * Stores a mutable mapping between a data source [[org.opencypher.caps.api.graph.Namespace]] and the specific [[org.opencypher.caps.api.io.PropertyGraphDataSource]].
    *
    * This mapping also holds the [[org.opencypher.caps.impl.io.SessionPropertyGraphDataSource]] by default.
    */
  protected var dataSourceMapping: Map[Namespace, PropertyGraphDataSource] =
    Map(sessionNamespace -> new SessionPropertyGraphDataSource)

  /**
    * Register the given [[org.opencypher.caps.api.io.PropertyGraphDataSource]] under the specific [[org.opencypher.caps.api.graph.Namespace]] within this session.
    *
    * This enables a user to refer to that [[org.opencypher.caps.api.io.PropertyGraphDataSource]] within a Cypher query.
    *
    * Note, that it is not allowed to overwrite an already registered [[org.opencypher.caps.api.graph.Namespace]]. Use [[CypherSession#delete]] first.
    *
    * @param namespace  namespace for lookup
    * @param dataSource property graph data source
    */
  def registerSource(namespace: Namespace, dataSource: PropertyGraphDataSource): Unit = dataSourceMapping.get(namespace) match {
    case Some(p) => throw IllegalArgumentException(s"no data source registered with namespace '$namespace'", p)
    case None => dataSourceMapping = dataSourceMapping.updated(namespace, dataSource)
  }

  /**
    * De-registers a [[org.opencypher.caps.api.io.PropertyGraphDataSource]] from the session by its given [[org.opencypher.caps.api.graph.Namespace]].
    *
    * @param namespace namespace for lookup
    */
  def deregisterSource(namespace: Namespace): Unit = {
    if (namespace == sessionNamespace) throw UnsupportedOperationException("de-registering the session data source")
    dataSourceMapping.get(namespace) match {
      case Some(_) => dataSourceMapping = dataSourceMapping - namespace
      case None => throw IllegalArgumentException(s"a data source registered with namespace '$namespace'")
    }
  }

  /**
    * Returns all [[org.opencypher.caps.api.graph.Namespace]]s registered at this session.
    *
    * @return registered namespaces
    */
  def namespaces: Set[Namespace] = dataSourceMapping.keySet

  /**
    * Returns the [[org.opencypher.caps.api.io.PropertyGraphDataSource]] that is registered under the given [[org.opencypher.caps.api.graph.Namespace]].
    *
    * @param namespace namespace for lookup
    * @return property graph data source
    */
  def dataSource(namespace: Namespace): PropertyGraphDataSource = dataSourceMapping.getOrElse(namespace,
    throw IllegalArgumentException(s"a data source registered with namespace '$namespace'"))

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  def cypher(query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords] = None): CypherResult

  /**
    * Stores the given [[org.opencypher.caps.api.graph.PropertyGraph]] to session-local storage under the given [[org.opencypher.caps.api.graph.GraphName]]. The specified graph
    * will be accessible under the session-local naming scheme, e.g. `session.graphName`.
    *
    * @param graphName graph name
    * @param graph     property graph to store
    */
  def store(graphName: GraphName, graph: PropertyGraph): QualifiedGraphName = {
    dataSourceMapping(SessionNamespace).store(graphName, graph)
    QualifiedGraphName(SessionNamespace, graphName)
  }

  /**
    * Stores the given [[org.opencypher.caps.api.graph.PropertyGraph]] using the [[org.opencypher.caps.api.io.PropertyGraphDataSource]] registered under the [[org.opencypher.caps.api.graph.Namespace]] of the
    * specified [[org.opencypher.caps.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @param graph              property graph to store
    */
  def store(qualifiedGraphName: QualifiedGraphName, graph: PropertyGraph): Unit =
    dataSource(qualifiedGraphName.namespace).store(qualifiedGraphName.graphName, graph)

  /**
    * Removes the [[org.opencypher.caps.api.graph.PropertyGraph]] associated with the given name from the session-local storage.
    *
    * @param graphName name of the graph within the session.
    */
  def delete(graphName: GraphName): Unit =
    dataSourceMapping(SessionNamespace).delete(graphName)

  /**
    * Removes the [[org.opencypher.caps.api.graph.PropertyGraph]] with the given qualified name from the data source associated with the specified
    * [[org.opencypher.caps.api.graph.Namespace]].
    *
    * @param qualifiedGraphName qualified graph name
    */
  def delete(qualifiedGraphName: QualifiedGraphName): Unit =
    dataSource(qualifiedGraphName.namespace).delete(qualifiedGraphName.graphName)

  /**
    * Returns the [[org.opencypher.caps.api.graph.PropertyGraph]] that is stored in session-local storage under the given [[org.opencypher.caps.api.graph.GraphName]].
    *
    * @param graphName qualified graph name
    * @return property graph
    */
  def graph(graphName: GraphName): PropertyGraph =
    dataSource(sessionNamespace).graph(graphName)

  /**
    * Returns the [[org.opencypher.caps.api.graph.PropertyGraph]] that is stored under the given [[org.opencypher.caps.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @return property graph
    */
  def graph(qualifiedGraphName: QualifiedGraphName): PropertyGraph =
    dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName)

  /**
    * Executes a Cypher query in this session, using the argument graph as the ambient graph.
    *
    * The ambient graph is the graph that is used for graph matching and updating,
    * unless another graph is explicitly selected by the query.
    *
    * @param graph      ambient graph for this query
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  private[graph] def cypherOnGraph(
    graph: PropertyGraph,
    query: String,
    parameters: CypherMap = CypherMap.empty,
    drivingTable: Option[CypherRecords]): CypherResult
}
