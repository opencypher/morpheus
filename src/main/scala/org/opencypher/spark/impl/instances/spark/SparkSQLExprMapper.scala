package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.impl.physical.RuntimeContext

object SparkSQLExprMapper {

  private def verifyExpression(header: RecordHeader, expr: Expr) = {
    val slots = header.slotsFor(expr)

    if (slots.isEmpty) {
      throw new IllegalStateException(s"No slot found for expression $expr")
    } else if (slots.size > 1) {
      throw new NotImplementedError("No support for multi-column expressions yet")
    }
  }

  private def getColumn(expr: Expr, header: RecordHeader, dataFrame: DataFrame): Column = {
    val slot = header.slotsFor(expr).head
    dataFrame.col(dataFrame.columns(slot.index))
  }

  /**
    * Attempts to create a Spark SQL expression from the SparkCypher expression.
    *
    * @param header  the header of the SparkCypherRecords in which the expression should be evaluated.
    * @param expr    the expression to be evaluated.
    * @param df      the dataframe containing the data over which the expression should be evaluated.
    * @param context context with helper functions, such as column names.
    * @return Some Spark SQL expression if the input was mappable, otherwise None.
    */
  def asSparkSQLExpr(header: RecordHeader, expr: Expr, df: DataFrame)
                    (implicit context: RuntimeContext): Option[Column] = expr match {

    // predicates
    case Not(Equals(v1: Var, v2: Var)) =>
      val lhsSlot = header.slotFor(v1)
      val rhsSlot = header.slotFor(v2)
      Some(new Column(df.columns(lhsSlot.index)) =!= new Column(df.columns(rhsSlot.index)))

    case Ands(exprs) =>
      val cols = exprs.map(asSparkSQLExpr(header, _, df))
      if (cols.contains(None)) None
      else {
        cols.reduce[Option[Column]] {
          case (Some(l: Column), Some(r: Column)) => Some(l && r)
          case _ => throw new IllegalStateException("This should never happen")
        }
      }

    case HasType(rel, relType) =>
      val relTypeId = context.tokens.relTypeRef(relType).id
      val idSlot = header.typeId(rel)
      Some(new Column(df.columns(idSlot.index)) === relTypeId)

    case h: HasLabel =>
      val slot = header.slotsFor(h).head
      Some(new Column(df.columns(slot.index))) // it's a boolean column

    // Arithmetics
    case add: Add =>
      verifyExpression(header, expr)

      val lhsColumn = getColumn(add.lhs, header, df)
      val rhsColumn = getColumn(add.rhs, header, df)
      Some(lhsColumn + rhsColumn)

    case sub: Subtract =>
      verifyExpression(header, expr)

      val lhsColumn = getColumn(sub.lhs, header, df)
      val rhsColumn = getColumn(sub.rhs, header, df)
      Some(lhsColumn - rhsColumn)

    case _ => None
  }

}
