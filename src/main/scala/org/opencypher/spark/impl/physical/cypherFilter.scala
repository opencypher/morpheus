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
package org.opencypher.spark.impl.physical

import org.apache.spark.sql.Row
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.instances.spark.RowUtils._
import org.opencypher.spark.api.value.CypherValueUtils._

/*
 * Used when the predicate depends on values not stored inside the dataframe.
 */
case class cypherFilter(header: RecordHeader, expr: Expr)
                       (implicit context: RuntimeContext) extends (Row => Option[Boolean]) {
  def apply(row: Row): Option[Boolean] = expr match {
    case Equals(p: Property, c: Const) =>
      // TODO: Make this ternary
      Some(row.getCypherValue(p, header) == row.getCypherValue(c, header))

    case LessThan(lhs, rhs) =>
      row.getCypherValue(lhs, header) < row.getCypherValue(rhs, header)

    case LessThanOrEqual(lhs, rhs) =>
      row.getCypherValue(lhs, header) <= row.getCypherValue(rhs, header)

    case GreaterThan(lhs, rhs) =>
      row.getCypherValue(lhs, header) > row.getCypherValue(rhs, header)

    case GreaterThanOrEqual(lhs, rhs) =>
      row.getCypherValue(lhs, header) >= row.getCypherValue(rhs, header)

    case x =>
      Raise.notYetImplemented(s"Predicate $x")
  }
}
