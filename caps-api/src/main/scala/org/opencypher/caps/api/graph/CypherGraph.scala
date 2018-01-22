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

import org.opencypher.caps.api.record.CypherRecords
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue

/**
  * A Property Graph as defined by the openCypher Property Graph Model.
  *
  * A graph is always tied to and managed by a session. The lifetime of a graph is bounded
  * by the session lifetime.
  *
  * A graph always has a schema, which describes the properties of the entities in the graph,
  * grouped by the labels and relationship types of the entities.
  *
  * @see [[https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc openCypher Property Graph Model]]
  */
trait CypherGraph {

  /**
    * The schema that describes this graph.
    *
    * @return the schema of this graph.
    */
  def schema: Schema

  /**
    * The session in which this graph is managed.
    *
    * @return the session of this graph.
    */
  def session: CypherSession

  def nodes(name: String): CypherRecords = nodes(name, CTNode)

  /**
    * Constructs a scan table of all the nodes in this graph with the given cypher type.
    *
    * @param name the field name for the returned nodes.
    * @return a table of nodes of the specified type.
    */
  def nodes(name: String, nodeCypherType: CTNode): CypherRecords

  def relationships(name: String): CypherRecords = relationships(name, CTRelationship)

  /**
    * Constructs a scan table of all the relationships in this graph with the given cypher type.
    *
    * @param name the field name for the returned relationships.
    * @return a table of relationships of the specified type.
    */
  def relationships(name: String, relCypherType: CTRelationship): CypherRecords

  /**
    * Executes a Cypher query in the session that manages this graph, using this graph as the ambient graph.
    *
    * @param query      the Cypher query to execute.
    * @param parameters the parameters used by the Cypher query.
    * @return           the result of the query.
    * @see              [[CypherSession.cypher()]]
    */
  final def cypher(query: String, parameters: Map[String, CypherValue] = Map.empty): CypherResult =
    session.cypher(graph, query, parameters)

  /**
    * Constructs the union of this graph and the argument graph.
    * The argument graph has to be managed by the same session as this graph.
    *
    * @param other the argument graph with which to union.
    * @return the union of this and the argument graph.
    */
  def union(other: CypherGraph): CypherGraph

  protected def graph: CypherGraph
}
