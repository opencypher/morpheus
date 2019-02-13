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
package org.opencypher.okapi.api.graph

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap

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
trait PropertyGraph {

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

  /**
    * Returns all nodes in this graph with the given [[org.opencypher.okapi.api.types.CTNode]] type.
    *
    * @param name            field name for the returned nodes
    * @param nodeCypherType  node type used for selection
    * @param exactLabelMatch return only nodes that have exactly the given labels
    * @return table of nodes of the specified type
    */
  def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): CypherRecords

  /**
    * Returns all relationships in this graph with the given [[org.opencypher.okapi.api.types.CTRelationship]] type.
    *
    * @param name          field name for the returned relationships
    * @param relCypherType relationship type used for selection
    * @return table of relationships of the specified type
    */
  def relationships(name: String, relCypherType: CTRelationship = CTRelationship): CypherRecords

  /**
    * Constructs the union of this graph and the argument graphs. Note that the argument graphs have to
    * be managed by the same session as this graph.
    *
    * This operation does not merge any nodes or relationships, but simply creates a new graph consisting
    * of all nodes and relationships of the argument graphs.
    *
    * @param others argument graphs with which to union
    * @return union of this and the argument graph
    */
  def unionAll(others: PropertyGraph*): PropertyGraph

  /**
    * Executes a Cypher query in the session that manages this graph, using this graph as the input graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query.
    */
  def cypher(
    query: String,
    parameters: CypherMap = CypherMap.empty,
    drivingTable: Option[CypherRecords] = None,
    queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty
  ): CypherResult = session.cypherOnGraph(this, query, parameters, drivingTable, queryCatalog)

  /**
    * Returns all patterns that the graph can provide
    *
    * @return patterns that the graph can provide
    */
  def patterns: Set[Pattern] =
    schema.labelCombinations.combos.map(c => NodePattern(CTNode(c))) ++
    schema.relationshipTypes.map(r => RelationshipPattern(CTRelationship(r)))
}
