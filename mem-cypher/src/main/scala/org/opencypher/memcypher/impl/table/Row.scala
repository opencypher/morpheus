package org.opencypher.memcypher.impl.table

import org.opencypher.memcypher.impl.types.CypherValueOps._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object Row {

  def apply(values: Any*): Row = Row(values.toArray)

  def fromSeq(values: Seq[Any]): Row = Row(values.toArray)

  implicit class RichMemRow(row: Row) {
    def evaluate(expr: Expr)(implicit header: RecordHeader, schema: Schema, parameters: CypherMap): Any = expr match {

      case Param(name) => parameters(name).value

      case _: Var | _: Param | _: Property | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
        row.get(schema.fieldIndex(header.column(expr)))

      case Equals(lhs, rhs) =>
        evaluate(lhs).toCypherValue == evaluate(rhs).toCypherValue

      case Not(inner) =>
        !evaluate(inner).toCypherValue

      case GreaterThan(lhs, rhs) =>
        evaluate(lhs).toCypherValue > evaluate(rhs).toCypherValue

      case GreaterThanOrEqual(lhs, rhs) =>
        evaluate(lhs).toCypherValue >= evaluate(rhs).toCypherValue

      case LessThan(lhs, rhs) =>
        evaluate(lhs).toCypherValue < evaluate(rhs).toCypherValue

      case LessThanOrEqual(lhs, rhs) =>
        evaluate(lhs).toCypherValue <= evaluate(rhs).toCypherValue

      case Ands(exprs) =>
        exprs.map(evaluate).map(_.toCypherValue).reduce(_ && _).cast[Boolean]

      case Ors(exprs) =>
        exprs.map(evaluate).map(_.toCypherValue).reduce(_ || _).cast[Boolean]

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

  def ++(other: Row): Row = copy(values = values ++ other.values)

  override def toString: String = values.mkString(", ")
}
