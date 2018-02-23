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
package org.opencypher.caps.api.table

import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.table.CypherTable

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  */
trait CypherRecords extends CypherTable[String] with CypherPrintable {

  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise the full result set.
    *
    * @note This method may be considerably slower than [[org.opencypher.caps.api.table.CypherRecords#collect]].
    *       Use this method only if collect could outgrow the available driver memory.
    */
  def iterator: Iterator[CypherMap]

  /**
    * Consume these records and collect them into an array.
    *
    * WARNING: This operation may be very expensive as it may have to materialise the full result set.
    */
  def collect: Array[CypherMap]

  /**
    * Registers these records as a table under the given name.
    *
    * @param name the name under which this table may be referenced.
    */
  def register(name: String): Unit
}
