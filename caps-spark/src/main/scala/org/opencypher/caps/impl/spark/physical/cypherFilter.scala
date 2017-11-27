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
package org.opencypher.caps.impl.spark.physical

import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.value.instances._
import org.opencypher.caps.api.value.syntax._
import org.opencypher.caps.impl.spark.DfUtils._
import org.opencypher.caps.impl.spark.exception.Raise

/*
 * Used when the predicate depends on values not stored inside the dataframe.
 */
case class cypherFilter(header: RecordHeader, expr: Expr)
                       (implicit context: RuntimeContext) extends (Row => Boolean) {
  def apply(row: Row): Boolean =
    expr match {
      case Equals(lhs, rhs) =>
        val lhsValue = row.getCypherValue(lhs, header)
        val rhsValue = row.getCypherValue(rhs, header)
        (lhsValue equalTo rhsValue).isTrue

      case LessThan(lhs, rhs) =>
        val lhsValue = row.getCypherValue(lhs, header)
        val rhsValue = row.getCypherValue(rhs, header)
        (lhsValue < rhsValue).orNull

      case LessThanOrEqual(lhs, rhs) =>
        val lhsValue = row.getCypherValue(lhs, header)
        val rhsValue = row.getCypherValue(rhs, header)
        (lhsValue <= rhsValue).orNull

      case GreaterThan(lhs, rhs) =>
        val lhsValue = row.getCypherValue(lhs, header)
        val rhsValue = row.getCypherValue(rhs, header)
        (lhsValue > rhsValue).orNull

      case GreaterThanOrEqual(lhs, rhs) =>
        val lhsValue = row.getCypherValue(lhs, header)
        val rhsValue = row.getCypherValue(rhs, header)
        (lhsValue >= rhsValue).orNull

      case x =>
        Raise.notYetImplemented(s"Predicate $x")
    }
}
