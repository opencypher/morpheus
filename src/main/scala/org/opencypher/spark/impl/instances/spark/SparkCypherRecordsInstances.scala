package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.{Column, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.value.CypherValueUtils._
import org.opencypher.spark.impl.classes.Transform
import org.opencypher.spark.impl.instances.spark.RowUtils._
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.physical.RuntimeContext

trait SparkCypherRecordsInstances extends Serializable {

  /*
   * Used when the predicate depends on values not stored inside the dataframe.
   */
  case class cypherFilter(header: RecordHeader, expr: Expr)
                         (implicit context: RuntimeContext) extends (Row => Option[Boolean]) {
    def apply(row: Row) = expr match {
      case Equals(p: Property, c: Const) =>
        val slot = header.slotsFor(p).headOption match {
          case Some(s) => s
          case None => throw new IllegalStateException(s"Expected to find $p in $header")
        }
        val lhs = row.getCypherValue(slot.index, slot.content.cypherType)
        val rhs = context.constants(c.ref)

        Some(lhs == rhs)
      case LessThan(lhs, rhs) =>
        val leftSlot = header.slotsFor(lhs).head
        val rightSlot = header.slotsFor(rhs).head
        val leftValue = row.getCypherValue(leftSlot.index, leftSlot.content.cypherType)
        val rightValue = row.getCypherValue(rightSlot.index, rightSlot.content.cypherType)

        leftValue < rightValue
      case x =>
        throw new NotImplementedError(s"Predicate $x not yet supported")
    }
  }

  implicit def sparkCypherRecordsTransform(implicit context: RuntimeContext) =
    new Transform[SparkCypherRecords] with Serializable {

      private def liftTernary(f: Row => Option[Boolean]): (Row => Boolean) = {
        (r: Row) =>
          f(r) match {
            case None => false
            case Some(x) => x
          }
      }

      override def filter(subject: SparkCypherRecords, expr: Expr, newHeader: RecordHeader): SparkCypherRecords = {

        val filteredRows = asSparkSQLExpr(subject.header, expr, subject.data) match {
          case Some(sqlExpr) =>
            subject.data.where(sqlExpr)
          case None =>
            val predicate = cypherFilter(newHeader, expr)
            subject.data.filter(liftTernary(predicate))
        }

        val selectedColumns = newHeader.slots.map { c =>
          val name = context.columnName(c)
          filteredRows.col(name)
        }

        val newData = filteredRows.select(selectedColumns: _*)

        SparkCypherRecords.create(newHeader, newData)(subject.session)
      }

      // TODO: Correctly handle aliasing in the header
      override def select(subject: SparkCypherRecords, fields: IndexedSeq[Var], newHeader: RecordHeader)
      : SparkCypherRecords = {
        val data = subject.data
        val columns = fields.map { f =>
          data.col(data.columns(subject.header.slotsFor(f).head.index))
        }
        val newData = subject.data.select(columns: _*)

        SparkCypherRecords.create(newHeader, newData)(subject.session)
      }

      override def reorder(subject: SparkCypherRecords, newHeader: RecordHeader): SparkCypherRecords = {
        val columns = newHeader.slots.map(context.columnName)

        val newData = subject.data.select(columns.head, columns.tail: _*)

        SparkCypherRecords.create(newHeader, newData)(subject.session)
      }

      override def alias2(subject: SparkCypherRecords, expr: Expr, v: Var, newHeader: RecordHeader)
      : SparkCypherRecords = {
        val oldSlot = subject.header.slotsFor(expr).head

        val newSlot = newHeader.slotsFor(v).head

        val oldColumnName = context.columnName(oldSlot)
        val newColumnName = context.columnName(newSlot)

        val newData = if (subject.data.columns.contains(oldColumnName)) {
          subject.data.withColumnRenamed(oldColumnName, newColumnName)
        } else {
          throw new IllegalStateException(s"Wanted to rename column $oldColumnName, but it was not present!")
        }

        SparkCypherRecords.create(newHeader, newData)(subject.session)
      }

      override def project(subject: SparkCypherRecords, expr: Expr, newHeader: RecordHeader): SparkCypherRecords = {

        val newData = asSparkSQLExpr(newHeader, expr, subject.data) match {
          case None => throw new NotImplementedError(s"No support for projecting $expr yet")

          case Some(sparkSqlExpr) =>
            val columnsToSelect = subject.data.columns.map(subject.data.col) :+ sparkSqlExpr
            subject.data.select(columnsToSelect: _*)
        }

        SparkCypherRecords.create(newHeader, newData)(subject.session)
      }

      override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords)
                       (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords =
        join(lhs, rhs, lhs.header ++ rhs.header)(lhsSlot, rhsSlot)

      override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords, jointHeader: RecordHeader)
                       (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords = {

        if (lhs.session == rhs.session) {
          val lhsData = lhs.data
          val rhsData = rhs.data

          val lhsColumn = lhsData.col(lhsData.columns(lhsSlot.index))
          val rhsColumn = rhsData.col(rhsData.columns(rhsSlot.index))

          val joinExpr: Column = lhsColumn === rhsColumn
          val jointData = lhsData.join(rhsData, joinExpr, "inner")

          SparkCypherRecords.create(jointHeader, jointData)(lhs.session)
        } else {
          throw new IllegalArgumentException("Cannot join records from different sessions")
        }
      }
    }
}
