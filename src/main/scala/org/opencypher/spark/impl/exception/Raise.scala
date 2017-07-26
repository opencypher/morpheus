/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.exception

import org.opencypher.spark.api.exception.SparkCypherException

object Raise {
  def duplicateEmbeddedEntityColumn(name: String) = throw SparkCypherException(
    "The input column '$name' is used more than once to describe an embedded entity"
  )

  def recordsDataHeaderMismatch() = throw SparkCypherException(
    "Column mismatch between data and header!"
  )

  def duplicateColumnNamesInData() = throw SparkCypherException(
    "Cannot use data frames with duplicate column names"
  )

  def invalidDataTypeForColumn(column: String, header: String, typ: String) = throw SparkCypherException(
    s"Invalid data type for column $column. Expected at least $header but got conflicting $typ"
  )

  def graphSpaceMismatch() = throw SparkCypherException(
    "Import of a data frame not created in the same session as the graph space"
  )

  def slotNotAdded(field: String) = throw SparkCypherException(
    s"Failed to add new slot: $field"
  )

  def slotNotFound(expr: String) = throw SparkCypherException(
    s"Did not find slot for $expr"
  )

  def multipleSlotsForExpression() = throw SparkCypherException(
    "Only a single slot per expression currently supported"
  )

  def notYetImplemented(what: String) = throw new NotImplementedError(
    s"Support for $what not yet implemented"
  )

  def columnNotFound(column: String) = throw SparkCypherException(
    s"Wanted to rename column $column, but it was not present!"
  )

  def invalidPattern(pattern: String) = throw SparkCypherException(
    s"What kind of a pattern is this??? $pattern"
  )

  def invalidConnection(endPoint: String) = throw SparkCypherException(
    s"A connection must have a known $endPoint!"
  )

  def patternPlanningFailure() = throw SparkCypherException(
    "Recursion / solved failure during logical planning: unable to find unsolved connection"
  )

  def logicalPlanningFailure() = throw SparkCypherException(
    "Error during logical planning"
  )

  def impossible() = throw SparkCypherException(
    "Something impossible happened!"
  )

  def invalidArgument(expected: String, actual: String) = throw SparkCypherException(
    s"Expected a $expected but got a $actual"
  )

  def typeInferenceFailed(detail: String) = throw SparkCypherException(
    s"Some error in type inference: $detail"
  )

  def schemaMismatch(detail: String) = throw SparkCypherException(
    s"Incompatible schemas: $detail"
  )
}
