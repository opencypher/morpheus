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
import org.opencypher.caps.impl.exception.IllegalArgumentException

/**
  * Represents a table with column names of type String in which each row contains one CypherValue per column and the
  * values in each column have the same Cypher type.
  *
  * This interface is used to access simple Cypher values from a table. When it is implemented with an entity mapping
  * it can also be used to assemble complex Cypher values such as CypherNode/CypherRelationship that are stored over
  * multiple columns in a low-level Cypher table.
  */
trait CypherTable[K] {

  def columns: Seq[K]

  def columnType: Map[K, CypherType]

  /**
    * Iterator over the rows in this table.
    */
  def rows: Iterator[K => CypherValue]

  /**
    * @return number of rows in this Table.
    */
  def size: Long

}

object CypherTable {

  implicit class RichCypherTable[K](table: CypherTable[K]) {

    /**
      * Checks if the data type of the given column is compatible with the expected type.
      *
      * @param columnKey    column to be checked
      * @param expectedType excepted data type
      */
    def verifyColumnType(columnKey: K, expectedType: CypherType, keyDescription: String): Unit = {
      val columnType = table.columnType(columnKey)
      if (!columnType.subTypeOf(expectedType).isTrue) {
        if (columnType.material == expectedType.material) {
          throw IllegalArgumentException(
            s"a non-nullable type for $keyDescription column `$columnKey`",
            "a nullable type")
        } else {
          throw IllegalArgumentException(
            s"$keyDescription column `$columnKey` of type $expectedType",
            s"incompatible column type $columnType")
        }
      }

    }

  }

}

trait CypherRecordsCompanion[R <: CypherRecords, S <: CypherSession] {
  def unit()(implicit session: S): R
}

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  */
trait CypherRecords extends CypherTable[String] with CypherPrintable {

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
