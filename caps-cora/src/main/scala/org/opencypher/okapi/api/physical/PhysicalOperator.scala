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
package org.opencypher.okapi.api.physical

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.table.RecordHeader

/**
  * Represents a backend-specific implementation of a physical query operation on the underlying data.
  *
  * @tparam R backend-specific cypher records
  * @tparam G backend-specific property graph
  * @tparam C backend-specific runtime context
  */
trait PhysicalOperator[R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]] {

  /**
    * The record header constructed by that operator.
    *
    * @return record header describing the output data
    */
  def header: RecordHeader

  /**
    * Triggers the execution of that operator.
    *
    * @param context backend-specific runtime context
    * @return physical result
    */
  def execute(implicit context: C): PhysicalResult[R, G]

}
