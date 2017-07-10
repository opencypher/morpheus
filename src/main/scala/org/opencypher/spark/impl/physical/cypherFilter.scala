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
