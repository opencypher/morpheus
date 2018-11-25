package org.opencypher.memcypher.impl.table

import org.opencypher.memcypher.impl.types.CypherValueOps._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object Row {

  def apply(values: Any*): Row = Row(values.toArray)

  def fromSeq(values: Seq[Any]): Row = Row(values.toArray)

  implicit class RichMemRow(row: Row) {
    def evaluate(expr: Expr)(implicit header: RecordHeader, schema: Schema, parameters: CypherMap): Any = expr match {

      case Param(name) => parameters(name).value

      case _: Var | _: Param | _: Property | _: HasLabel | _: HasType | _: StartNode | _: EndNode =>
        row.get(schema.fieldIndex(header.column(expr)))

      case AliasExpr(inner, _) =>
        evaluate(inner)

      case Labels(inner) =>
        inner match {
          case v: Var =>
            val labels = header.labelsFor(v).toSeq.map(e => e.label.name -> evaluate(e)).sortBy(_._1)
            labels.collect { case (label, isPresent: Boolean) if isPresent => label }
          case _ =>
            throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
        }

      case Type(inner) =>
        inner match {
          case v: Var =>
            val relTypes = header.typesFor(v).toSeq.map(e => e.relType.name -> evaluate(e))
            relTypes.collectFirst { case (relType, isPresent: Boolean) if isPresent => relType }.orNull
          case _ =>
            throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
        }

      case IsNull(inner) =>
        evaluate(inner) == null

      case IsNotNull(inner) =>
        evaluate(inner) != null

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

      case _: NullLit => null

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
