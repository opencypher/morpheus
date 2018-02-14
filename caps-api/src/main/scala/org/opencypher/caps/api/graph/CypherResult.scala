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

import org.opencypher.caps.api.table.{CypherPrintable, CypherRecords}
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.trees.TreeNode

/**
  * Describes the result of executing a Cypher query.
  *
  * The result of a Cypher query consists of a table of records and a set of named graphs.
  */
trait CypherResult extends CypherPrintable {

  /**
    * Retrieves the graph returned by the query. Note, if the query returns more than one graph, there is no guarantee
    * about which graph is returned.
    *
    * @return a graph
    */
  def graph: Option[PropertyGraph] = graphs.headOption.map(_._2)

  /**
    * The named graphs that were returned by the query that produced this result.
    *
    * @return a map of named graphs.
    */
  def graphs: Map[String, PropertyGraph]

  /**
    * The table of records that was returned by the query that produced this result.
    *
    * @return a table of records.
    */
  def records: CypherRecords

  /**
    * API for printable plans. This is used for explaining the execution plan of a Cypher query.
    */
  def plans: CypherQueryPlans

}

trait CypherQueryPlans {
  def logical: CypherPrintable
  def physical: CypherPrintable
}
