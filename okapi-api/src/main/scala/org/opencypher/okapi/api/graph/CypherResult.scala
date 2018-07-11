/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.table.{CypherPrintable, CypherRecords}

/**
  * Describes the result of executing a Cypher query.
  *
  * The result of a Cypher query consists of a table of records and a set of named graphs.
  */
// TODO: make graph and records non-optional
trait CypherResult extends CypherPrintable {

  type Graph = PropertyGraph

  /**
    * Retrieves the graph if one is returned by the query.
    * If the query returns a table, `None` is returned.
    *
    * @return a graph if the query returned one, `None` otherwise
    */
  def getGraph: Option[Graph]

  /**
    * Retrieves the graph if one is returned by the query, otherwise an exception is thrown.
    *
    * @return graph as returned by the query.
    */
  def graph: Graph = getGraph.get

  /**
    * The table of records if one was returned by the query.
    * Returns `None` if the query returned a graph.
    *
    * @return a table of records, `None` otherwise.
    */
  def getRecords: Option[CypherRecords]

  /**
    * The table of records if one was returned by the query, otherwise an exception is thrown.
    *
    * @return a table of records.
    */
  def records: CypherRecords = getRecords.get

  /**
    * API for printable plans. This is used for explaining the execution plan of a Cypher query.
    */
  def plans: CypherQueryPlans

}

trait CypherQueryPlans {
  def logical: String

  def relational: String
}
