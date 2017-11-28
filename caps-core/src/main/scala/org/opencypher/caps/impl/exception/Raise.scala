/*
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
package org.opencypher.caps.impl.exception

import java.net.URI

import org.neo4j.cypher.internal.frontend.v3_3.SemanticErrorDef
import org.opencypher.caps.api.exception.CAPSException

object Raise {

  def invalidEmptyLabel() = throw CAPSException(
    "A label can not be empty"
  )

  def invalidSchema(label: String, key: Any, t1: Any, t2: Any) = throw CAPSException(
    s"The schema had a conflict on key '$key': $t1 and $t2 (for label $label)"
  )

  def invalidSchemaAddition(key: Any, t1: Any, t2: Any) = throw CAPSException(
    s"Attempt to overwrite key '$key': had $t1 and got $t2"
  )

  def duplicateEmbeddedEntityColumn(name: String) = throw CAPSException(
    "The input column '$name' is used more than once to describe an embedded entity"
  )

  def recordsDataHeaderMismatch(header: String, data: String) = throw CAPSException(
    s"Column mismatch between header $header and data $data"
  )

  def duplicateColumnNamesInData() = throw CAPSException(
    "Cannot use data frames with duplicate column names"
  )

  def invalidDataTypeForColumn(column: String, header: String, typ: String) = throw CAPSException(
    s"Invalid data type for column $column. Expected at least $header but got conflicting $typ"
  )

  def capsSessionMismatch() = throw CAPSException(
    "Import of a data frame from a different session"
  )

  def schemaMismatch() = throw CAPSException(
    "Loaded graph with a mismatching schema that differs from the schema loaded during logical planning"
  )

  def slotNotAdded(field: String) = throw CAPSException(
    s"Failed to add new slot: $field"
  )

  def slotNotFound(expr: String) = throw CAPSException(
    s"Did not find slot for $expr"
  )

  def multipleSlotsForExpression() = throw CAPSException(
    "Only a single slot per expression currently supported"
  )

  def notYetImplemented(what: String) = throw new NotImplementedError(
    s"Support for $what not yet implemented"
  )

  def columnNotFound(column: String) = throw CAPSException(
    s"Wanted to rename column $column, but it was not present!"
  )

  def invalidOrUnsupportedPattern(pattern: String) = throw CAPSException(
    s"Unsupported or invalid pattern. Can't plan $pattern"
  )

  def invalidConnection(endPoint: String) = throw CAPSException(
    s"A connection must have a known $endPoint!"
  )

  def patternPlanningFailure() = throw CAPSException(
    "Recursion / solved failure during logical planning: unable to find unsolved connection"
  )

  def logicalPlanningFailure() = throw CAPSException(
    "Error during logical planning"
  )

  def impossible(detail: String = "") = throw CAPSException(
    s"Something impossible happened! $detail"
  )

  def invalidArgument(expected: String, actual: Any) = throw CAPSException(
    s"Expected $expected but found $actual"
  )

  def unsupportedArgument(actual: String) = throw CAPSException(
    s"Cannot (yet) handle the given argument $actual"
  )

  def typeInferenceFailed(detail: String) = throw CAPSException(
    s"Some error in type inference: $detail"
  )

  def schemaMismatch(detail: String) = throw CAPSException(
    s"Incompatible schemas: $detail"
  )

  def semanticErrors(errors: Seq[SemanticErrorDef]): Unit = if (errors.nonEmpty) throw CAPSException(
    s"Errors during semantic checking: ${errors.mkString(", ")}"
  )

  def graphURIMalformedForUseBy(uri: URI, name: String) = throw CAPSException(
    s"Graph URI $uri is malformed for use by $name"
  )

  def graphSourceSchemeNotSupported(uri: URI, schemes: Set[String]) = throw CAPSException(
    s"Cannot create a graph source for $uri due to an unsupported scheme ${uri.getScheme} " +
    s"(supported schemes: ${schemes.toSeq.sorted.mkString(", ")})"
  )

  def graphNotFound(uri: URI) = throw CAPSException(
    s"Graph not found at $uri"
  )

  def graphAlreadyExists(uri: URI) = throw CAPSException(
    s"Cannot create new graph. There already exists a graph at $uri"
  )
}
