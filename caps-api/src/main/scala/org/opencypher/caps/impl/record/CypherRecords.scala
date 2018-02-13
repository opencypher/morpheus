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

import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}

/**
  * Represents a table with column names of type String in which each row contains one CypherValue per column and the
  * values in each column have the same Cypher type.
  *
  * This interface is used to access simple Cypher values from a table. When it is implemented with an entity mapping
  * it can also be used to assemble complex Cypher values such as CypherNode/CypherRelationship that are stored over
  * multiple columns in a low-level Cypher table.
  */
trait CypherTable {

  def columns: Set[String]

  def columnType: Map[String, CypherType]

  /**
    * Iterator over the rows in this table.
    */
  def rows: Iterator[String => CypherValue]

  /**
    * @return number of rows in this Table.
    */
  def size: Long

}

// TODO: Remove
trait CypherRecordHeader {
  def fields: Set[String]

  def fieldsInOrder: Seq[String]
}

trait CypherRecordsCompanion[R <: CypherRecords, S <: CypherSession] {
  def unit()(implicit session: S): R
}

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  *
  * Slots that have been bound to a variable name are called <i>fields</i>.
  *
  * @see [[CypherRecordHeader]]
  */
//TODO: Move to API package
trait CypherRecords extends CypherTable with CypherPrintable {

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

  /**
    * Registers these records as a table under the given name.
    *
    * @param name the name under which this table may be referenced.
    */
  def register(name: String): Unit
}
