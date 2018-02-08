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
package org.opencypher.caps.impl.record

import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}


/**
  * Represents a table in which each row contains Vs.
  * Each row contains one V per column.
  */
trait Table[+V] {

  def columns: Set[String]

  /**
    * Iterate over the rows in this table.
    */
  def rows: Iterator[String => V]

  /**
    * @return number of rows in this Table.
    */
  def size: Long

}

//TODO: Move and rename
trait CypherRecordHeader {
  def fields: Set[String]

  def fieldsInOrder: Seq[String]
}

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  *
  * Slots that have been bound to a variable name are called <i>fields</i>.
  *
  * @see [[CypherRecordHeader]]
  */
trait CypherRecords extends Table[CypherValue] with CypherPrintable {

  /**
    * The header for this table, describing the slots stored.
    *
    * @return the header for this table.
    */
  def header: CypherRecordHeader

  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise
    */
  def iterator: Iterator[CypherMap]

  /**
    * @return the number of records in this CypherRecords.
    */
  def size: Long
}
