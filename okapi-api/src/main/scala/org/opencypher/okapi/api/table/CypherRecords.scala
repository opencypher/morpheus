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
package org.opencypher.okapi.api.table

import org.opencypher.okapi.api.graph.CypherSession
import org.opencypher.okapi.api.value.CypherValue.CypherMap

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  */
trait CypherRecords extends CypherTable with CypherPrintable {

  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise the full result set.
    *
    * @note This method may be considerably slower than [[org.opencypher.okapi.api.table.CypherRecords#collect]].
    *       Use this method only if collect could outgrow the available driver memory.
    */
  def iterator: Iterator[CypherMap]

  /**
    * Consume these records and collect them into an array.
    *
    * WARNING: This operation may be very expensive as it may have to materialise the full result set.
    */
  def collect: Array[CypherMap]

}

trait CypherRecordsCompanion[R <: CypherRecords, S <: CypherSession] {
  def unit()(implicit session: S): R
}
