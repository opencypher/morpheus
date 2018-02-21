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
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource.{Namespace => SessionNamespace}

// TODO: extend doc with explanation for writing graphs
/**
  * The Cypher Session is the main API for a Cypher-based application. It manages graphs which can be queried using
  * Cypher. Graphs can be read from different data sources (e.g. CSV) and mounted in the session-local storage.
  */
trait CypherSession {

  /**
    * Stores a mutable mapping between a data source [[Namespace]] and the specific [[PropertyGraphDataSource]].
    *
    * This mapping also holds the [[SessionPropertyGraphDataSource]] by default.
    */
  protected var dataSourceMapping: Map[Namespace, PropertyGraphDataSource] =
    Map(SessionNamespace -> new SessionPropertyGraphDataSource)

  /**
    * Register the given [[PropertyGraphDataSource]] under the specific [[Namespace]] within this session.
    *
    * This enables a user to refer to that [[PropertyGraphDataSource]] within a Cypher query.
    *
    * @param namespace  namespace for lookup
    * @param dataSource property graph data source
    */
  def register(namespace: Namespace, dataSource: PropertyGraphDataSource): Unit =
    dataSourceMapping = dataSourceMapping.updated(namespace, dataSource)

  /**
    * Returns the [[PropertyGraphDataSource]] that is registered under the given [[Namespace]].
    *
    * @param namespace namespace for lookup
    * @return property graph data source
    */
  def dataSource(namespace: Namespace): PropertyGraphDataSource = dataSourceMapping.getOrElse(namespace,
    throw IllegalArgumentException(s"a property graph data source registered with namespace '$namespace'"))

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  def cypher(query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords] = None): CypherResult

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
  private[graph] def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords]): CypherResult

  /**
    * Mounts the given property graph to session-local storage under the given name. The specified graph will be
    * accessible under the session-local naming scheme, e.g. {{{session.$graphName}}}.
    *
    * @param graph     property graph to register
    * @param graphName name of the graph within the session {{{session.graphName}}}
    */
  def mount(graphName: GraphName, graph: PropertyGraph): QualifiedGraphName = {
    dataSourceMapping(SessionNamespace).store(graphName, graph)
    QualifiedGraphName(SessionNamespace, graphName)
  }

  /**
    * Unmounts the property graph associated with the given name from the session-local storage.
    *
    * @param graphName name of the graph within the session {{{session.graphName}}}
    */
  def unmount(graphName: GraphName): Unit =
    dataSourceMapping(SessionNamespace).delete(graphName)

  /**
    * Unmounts all property graphs from the session-local storage.
    */
  def unmountAll(): Unit =
    dataSourceMapping(SessionNamespace).graphNames.foreach(unmount)

  /**
    * Returns the [[PropertyGraph]] that is registered under the given [[QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @return property graph
    */
  def catalog(qualifiedGraphName: QualifiedGraphName): PropertyGraph =
    dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName)

  /**
    * Writes the given [[PropertyGraph]] to the data source using the specified [[QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @param graph              property graph to write
    */
  // TODO: Error handling via Return Type (Success, Failed) or just throw exception?
  def write(qualifiedGraphName: QualifiedGraphName, graph: PropertyGraph): Unit =
    dataSource(qualifiedGraphName.namespace).store(qualifiedGraphName.graphName, graph)

}
