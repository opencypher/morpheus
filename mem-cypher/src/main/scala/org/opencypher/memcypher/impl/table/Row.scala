package org.opencypher.memcypher.impl.table

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, FalseLit, Param, TrueLit}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object Row {
  def apply(values: Any*): Row = Row(values.toArray)

  implicit class RichMemRow(row: Row) {
    def evaluate(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): Any = expr match {
      case Param(name) => parameters(name).value

      case TrueLit => true

      case FalseLit => false

      case other => throw UnsupportedOperationException(s"Evaluating expression $other is not supported.")
    }
  }
}

// Can't use Array[CypherValue] here, since using value classes in arrays enforces allocation:
// see https://docs.scala-lang.org/overviews/core/value-classes.html
case class Row(values: Array[Any]) extends AnyVal {

  def get(i: Int): Any = values(i)

  def getAs[T](i: Int): T = get(i).asInstanceOf[T]
}
