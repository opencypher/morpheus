package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.{Column, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.impl.classes.Transform
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.physical.RuntimeContext

trait SparkCypherRecordsInstances extends Serializable {

  implicit def sparkCypherRecordsTransform(implicit context: RuntimeContext) =
    new Transform[SparkCypherRecords] with Serializable {

      private def liftTernary(f: Row => Option[Boolean]): (Row => Boolean) = {
        (r: Row) =>
          f(r) match {
            case None => false
            case Some(x) => x
          }
      }

      override def reorder(subject: SparkCypherRecords, newHeader: RecordHeader): SparkCypherRecords = {
        val columns = newHeader.slots.map(context.columnName)

        val newData = subject.data.select(columns.head, columns.tail: _*)

        SparkCypherRecords.create(newHeader, newData)(subject.space)
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
          Raise.columnNotFound(oldColumnName)
        }

        SparkCypherRecords.create(newHeader, newData)(subject.space)
      }

      override def project(subject: SparkCypherRecords, expr: Expr, newHeader: RecordHeader): SparkCypherRecords = {

        val newData = asSparkSQLExpr(newHeader, expr, subject.data) match {
          case None => Raise.notYetImplemented(s"projecting $expr")

          case Some(sparkSqlExpr) =>
            val headerNames = newHeader.slotsFor(expr).map(context.columnName)
            val dataNames = subject.data.columns.toSeq

            // TODO: Can optimise for var AS var2 case -- avoid duplicating data
            headerNames.diff(dataNames) match {
              case Seq(one) =>
                // align the name of the column to what the header expects
                val newCol = sparkSqlExpr.as(one)
                val columnsToSelect = subject.data.columns.map(subject.data.col) :+ newCol

                subject.data.select(columnsToSelect: _*)
              case _ => Raise.multipleSlotsForExpression()
            }
        }

        SparkCypherRecords.create(newHeader, newData)(subject.space)
      }

      override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords)
                       (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords =
        join(lhs, rhs, lhs.header ++ rhs.header)(lhsSlot, rhsSlot)

      override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords, jointHeader: RecordHeader)
                       (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords = {

        if (lhs.space == rhs.space) {
          val lhsData = lhs.data
          val rhsData = rhs.data

          val lhsColumn = lhsData.col(context.columnName(lhsSlot))
          val rhsColumn = rhsData.col(context.columnName(rhsSlot))

          val joinExpr: Column = lhsColumn === rhsColumn
          val jointData = lhsData.join(rhsData, joinExpr, "inner")

          SparkCypherRecords.create(jointHeader, jointData)(lhs.space)
        } else {
          Raise.graphSpaceMismatch()
        }
      }
    }
}
