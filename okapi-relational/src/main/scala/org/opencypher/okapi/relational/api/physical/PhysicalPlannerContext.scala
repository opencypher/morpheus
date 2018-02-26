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
package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner

/**
  * Represents a back-end specific context which is used by the [[PhysicalPlanner]].
  *
  * @tparam R backend-specific cypher records
  */
trait PhysicalPlannerContext[R <: CypherRecords] {
  /**
    * Refers to the session in which that query is executed.
    *
    * @return back-end specific cypher session
    */
  def session: CypherSession

  /**
    * Lookup function that resolves URIs to property graphs.
    *
    * @return lookup function
    */
  def resolver: QualifiedGraphName => PropertyGraph

  /**
    * Initial records for physical planning.
    *
    * @return
    */
  def inputRecords: R

  /**
    * Query parameters
    *
    * @return query parameters
    */
  def parameters: CypherMap
}
