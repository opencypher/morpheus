package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.CTInteger
import org.opencypher.spark.impl.classes.Transform
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.physical.RuntimeContext

import scala.collection.mutable

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

      override def varExpand(lhs: SparkCypherRecords, rels: SparkCypherRecords, lower: Int, upper: Int, header: RecordHeader)
                            (nodeSlot: RecordSlot, startSlot: RecordSlot, rel: Var, path: Var): SparkCypherRecords = {
        val steps = new mutable.HashMap[Int, DataFrame]
        val startPoints = new mutable.HashMap[Int, Column]

        val nodeData = lhs.data
        val relsData = rels.data

        val keep = nodeData.columns.map(nodeData.col)

        val pathColName = context.columnName(OpaqueField(path))
        val edgeListColumn = udf(initArray _, ArrayType(LongType))()
        val lhsWithEmptyArray = nodeData.withColumn(pathColName, edgeListColumn)
        val expandCol = lhsWithEmptyArray.col(context.columnName(nodeSlot))

        val endNodeIdColName = "inventedName"
        val cols = nodeData.columns.map(nodeData.col) ++ Seq(lhsWithEmptyArray.col(pathColName), nodeData.col(context.columnName(nodeSlot)).as(endNodeIdColName))
        val withEndCol = lhsWithEmptyArray.select(cols: _*)
        steps(0) = withEndCol
        startPoints(0) = expandCol

        (1 to upper).foreach { i =>
          val (nextStart, result) = iterate(startPoints(i - 1), startSlot, rel, path, steps(i - 1), relsData, pathColName, endNodeIdColName, keep)

          // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
          steps(i) = result
          startPoints(i) = nextStart
        }

        // TODO: respect lower bound > 1
        val union = steps.values.reduce[DataFrame] {
          case (l, r) => l.union(r)
        }

        val renamed = union.withColumnRenamed(endNodeIdColName, context.columnName(ProjectedExpr(EndNode(rel)(CTInteger))))

        SparkCypherRecords.create(header, renamed)(lhs.space)
      }

      private def iterate(expandColumn: Column, startId: RecordSlot, rel: Var, path: Var, lhs: DataFrame, rels: DataFrame, pathColName: String, endNodeIdColName: String, keep: Seq[Column]): (Column, DataFrame) = {

        val relIdColumn = rels.col(context.columnName(OpaqueField(rel)))
        val startColumn = rels.col(context.columnName(startId))

        val join1 = lhs.join(rels, expandColumn === startColumn, "inner")

        val appendUdf = udf(arrayAppend _, ArrayType(LongType))
        val extendedArray = appendUdf(lhs.col(pathColName), relIdColumn)
        val tempArrayColName = "temp" // TODO watch out for already existing names!
        val withExtendedArray = join1.withColumn(tempArrayColName, extendedArray)
        val tempColumn = withExtendedArray.col(tempArrayColName)
        val arrayContains = udf(contains _, BooleanType)(tempColumn, relIdColumn)
        val filtered = withExtendedArray.filter(arrayContains)

        val endNodeIdColumn = filtered.col(context.columnName(ProjectedExpr(EndNode(rel)(CTInteger)))).as(endNodeIdColName)

        val columns = keep ++ Seq(tempColumn.as(pathColName), endNodeIdColumn)
        val result1 = filtered.select(columns: _*)

        endNodeIdColumn -> result1
      }

      private def initArray(): Any = {
        Array[Long]()
      }

      private def arrayAppend(array: Any, next: Any): Any = {
        array match {
          case a:mutable.WrappedArray[Long] => a :+ next
        }
      }

      private def contains(array: Any, elem: Any): Any = {
        array match {
          case a:mutable.WrappedArray[Long] => a.contains(elem)
        }
      }
    }
}
