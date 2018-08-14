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
package org.opencypher.okapi.relational.api.table

import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types.{CTNull, CypherType}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

/**
  * Main abstraction of a relational backend. A table represents a relation in terms of relational algebra and exposes
  * relational and additional auxiliary operators. Operators need to be implemented by the specific backend
  * (e.g. spark-cypher).
  *
  * @tparam T backend-specific representation of that table (e.g. DataFrame for spark-cypher)
  */
trait Table[T <: Table[T]] extends CypherTable {

  this: T =>

  /**
    * If supported by the backend, calling that operator caches the underlying table within the backend runtime.
    *
    * @return cached version of that table
    */
  def cache(): T = this

  /**
    * Returns a table containing only the given columns. The column order within the table is aligned with the argument.
    *
    * @param cols columns to select
    * @return table containing only requested columns
    */
  def select(cols: String*): T

  /**
    * Returns a table containing only rows where the given expression evaluates to true.
    *
    * @param expr       filter expression
    * @param header     table record header
    * @param parameters query parameters
    * @return table with filtered rows
    */
  def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): T

  /**
    * Returns a table with the given columns removed.
    *
    * @param cols columns to drop
    * @return table with dropped columns
    */
  def drop(cols: String*): T

  /**
    * Joins the current table with the given table on the specified join columns using equi-join semantics.
    *
    * @param other    table to join
    * @param joinType join type to perform (e.g. inner, outer, ...)
    * @param joinCols columns to join the two tables on
    * @return joined table
    */
  def join(other: T, joinType: JoinType, joinCols: (String, String)*): T

  /**
    * Computes the union of the current table and the given table. Requires both tables to have identical column layouts.
    *
    * @param other table to union with
    * @return union table
    */
  def unionAll(other: T): T

  /**
    * Returns a table that is ordered by the given columns.
    *
    * @param sortItems a sequence of column names and their order (i.e. ascending / descending)
    * @return ordered table
    */
  def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): T

  /**
    * Returns a table without the first n rows of the current table.
    *
    * @param n number of rows to skip
    * @return table with skipped rows
    */
  def skip(n: Long): T

  /**
    * Returns a table containing the first n rows of the current table.
    *
    * @param n number of rows to return
    * @return table with at most n rows
    */
  def limit(n: Long): T

  /**
    * Returns a table where each row is unique.
    *
    * @return table with unique rows
    */
  def distinct: T

  /**
    * Convenience operator for select and distinct.
    *
    * @param cols columns to select and perform distinct on.
    * @return table containing the specific columns and distinct rows
    */
  def distinct(cols: String*): T = select(cols: _*).distinct

  /**
    * Groups the rows within the table by the given query variables. Additionally a set of aggregations can be performed
    * on the grouped table.
    *
    * @param by           query variables to group by (e.g. (n)), if empty, the whole row is used as grouping key
    * @param aggregations set of aggregations functions and the column to store the result in
    * @param header       table record header
    * @param parameters   query parameters
    * @return table grouped by the given keys and results of possible aggregate functions
    */
  def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
    (implicit header: RecordHeader, parameters: CypherMap): T

  /**
    * Returns a table with additional expressions, which are evaluated and stored in the specified columns.
    *
    * @note If the column already exists, its contents will be replaced.
    * @param columns             tuples of expressions to evaluate and corresponding column name
    * @param header              table record header
    * @param parameters          query parameters
    * @return
    */
  def withColumns(columns: (Expr, String)*)(implicit header: RecordHeader, parameters: CypherMap): T

  /**
    * Returns a table with a renamed column name.
    *
    * @param oldColumn current column name
    * @param newColumn new column name
    * @return table with renamed column
    */
  def withColumnRenamed(oldColumn: String, newColumn: String): T

  /**
    * Prints the table to the system console.
    *
    * @param rows number of rows to print
    */
  def show(rows: Int = 20): Unit
}
