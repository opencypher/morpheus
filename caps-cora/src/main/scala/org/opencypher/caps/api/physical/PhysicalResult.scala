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
package org.opencypher.caps.api.physical

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.table.CypherRecords

/**
  * Represents a back-end specific physical result that is being produced by a [[PhysicalOperator]].
  *
  * @tparam R backend-specific cypher records
  * @tparam G backend-specific property graph
  */
trait PhysicalResult[R <: CypherRecords, G <: PropertyGraph] {

  /**
    * Performs the given function on the underlying records and returns the updated records.
    *
    * @param f map function
    * @return updated result
    */
  def mapRecordsWithDetails(f: R => R): PhysicalResult[R, G]

  /**
    * Stores the given graph identifed by the specified name in the result.
    *
    * @param t tuple mapping a graph name to a graph
    * @return updated result
    */
  // TODO: Remove (Update: this seems to be necessary for 'Define a new working graph and continue executing the query on it')
  def withGraph(t: (String, G)): PhysicalResult[R, G]

  /**
    * Returns a result that only contains the graphs with the given names.
    *
    * @param names graphs to select
    * @return updated result containing only selected graphs
    */
  // TODO: Remove
  def selectGraphs(names: Set[String]): PhysicalResult[R, G]
}
