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
import org.opencypher.caps.api.io.QualifiedGraphName
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue.CypherMap

/**
  * Represents a back-end specific runtime context that is being used by [[PhysicalOperator]] implementations.
  *
  * @tparam R backend-specific cypher records
  * @tparam G backend-specific property graph
  */
trait RuntimeContext[R <: CypherRecords, G <: PropertyGraph] {

  /**
    * Returns the graph referenced by the given [[QualifiedGraphName]].
    *
    * @return back-end specific property graph
    */
  def resolve: QualifiedGraphName => Option[G]

  /**
    * Query parameters
    *
    * @return query parameters
    */
  def parameters: CypherMap
}
