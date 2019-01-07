/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.IllegalArgumentException

/**
  * Represents a table in which each row contains one [[org.opencypher.okapi.api.value.CypherValue]] per column and the
  * values in each column have the same Cypher type.
  *
  * This interface is used to access simple Cypher values from a table. When it is implemented with an entity mapping
  * it can also be used to assemble complex Cypher values such as CypherNode/CypherRelationship that are stored over
  * multiple columns in a low-level Cypher table.
  */
trait CypherTable {

  /**
    * Physical column names in this table.
    *
    * Note, that there might be less physical then logical columns due to aliasing.
    */
  def physicalColumns: Seq[String]

  /**
    * Logical column names in this table as requested by a RETURN statement.
    */
  def logicalColumns: Option[Seq[String]] = None

  /**
    * Get the names of the physical columns that hold the values of the given return item.
    * A return item is given by an identifier or alias of a return clause. For example
    * in the query 'MATCH (n) RETURN n.foo' the only return item would be 'n.foo'
    *
    * It returns a list with a single value if the return item is a primitive. It will return
    * a list of column names if the return item is an entity, such as a node. The listed columns
    * hold the members of the entity (ids, label/type, properties) using an internal naming scheme.
    *
    * @param returnItem name of one of the return items represented in this table.
    * @return a list of names of the physical columns that hold the data for the return item.
    */
  def columnsFor(returnItem: String): Set[String]

  /**
    * CypherType of columns stored in this table.
    */
  def columnType: Map[String, CypherType]

  /**
    * Iterator over the rows in this table.
    */
  def rows: Iterator[String => CypherValue]

  /**
    * Number of rows in this Table.
    */
  def size: Long

}

object CypherTable {

  implicit class RichCypherTable(table: CypherTable) {

    /**
      * Checks if the data type of the given column is compatible with the expected type.
      *
      * @param columnKey    column to be checked
      * @param expectedType excepted data type
      */
    def verifyColumnType(columnKey: String, expectedType: CypherType, keyDescription: String): Unit = {
      val columnType = table.columnType.getOrElse(columnKey, throw IllegalArgumentException(
        s"table with column key $columnKey",
        s"table with columns ${table.physicalColumns.mkString(", ")}"))

      if (columnType.material.subTypeOf(expectedType.material).isFalse) {
        throw IllegalArgumentException(
          s"$keyDescription column `$columnKey` of type $expectedType",
          s"incompatible column type $columnType")
      }
    }
  }

}
