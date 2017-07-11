package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.impl.classes.Transform
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.flat.FreshVariableNamer
import org.opencypher.spark.impl.physical.RuntimeContext

import scala.collection.mutable

trait SparkCypherRecordsInstances extends Serializable {

  implicit def sparkCypherRecordsTransform(implicit context: RuntimeContext) =
    new Transform[SparkCypherRecords] with Serializable {

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

      override def initVarExpand(in: SparkCypherRecords, source: RecordSlot, edgeList: RecordSlot,
                                 target: RecordSlot, header: RecordHeader): SparkCypherRecords = {
        val inputData = in.data
        val keep = inputData.columns.map(inputData.col)

        val edgeListColName = context.columnName(edgeList)
        val edgeListColumn = udf(initArray _, ArrayType(LongType))()
        val withEmptyList = inputData.withColumn(edgeListColName, edgeListColumn)

        val cols = keep ++ Seq(
          withEmptyList.col(edgeListColName),
          inputData.col(context.columnName(source)).as(context.columnName(target)))

        val initializedData = withEmptyList.select(cols: _*)

        SparkCypherRecords.create(header, initializedData)(in.space)
      }

      override def varExpand(lhs: SparkCypherRecords, rels: SparkCypherRecords, lower: Int, upper: Int, header: RecordHeader)
                            (edgeList: Var, endNode: RecordSlot, rel: Var, relStartNode: RecordSlot): SparkCypherRecords = {
        val initData = lhs.data
        val relsData = rels.data

        val edgeListColName = context.columnName(lhs.header.slotFor(edgeList))

        val steps = new mutable.HashMap[Int, DataFrame]
        steps(0) = initData

        val keep = initData.columns

        val listTempColName = FreshVariableNamer.generateUniqueName(lhs.header)

        (1 to upper).foreach { i =>
          // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
          steps(i) = iterate(steps(i - 1), relsData)(endNode, rel, relStartNode, listTempColName, edgeListColName, keep)
        }

        val union = steps.filterKeys(_ >= lower).values.reduce[DataFrame] {
          case (l, r) => l.union(r)
        }

        SparkCypherRecords.create(lhs.header, union)(lhs.space)
      }

      private def iterate(lhs: DataFrame, rels: DataFrame)
                         (endNode: RecordSlot, rel: Var, relStartNode: RecordSlot,
                          listTempColName: String, edgeListColName: String, keep: Array[String]): DataFrame = {

        val relIdColumn = rels.col(context.columnName(OpaqueField(rel)))
        val startColumn = rels.col(context.columnName(relStartNode))
        val expandColumnName = context.columnName(endNode)
        val expandColumn = lhs.col(expandColumnName)

        val joined = lhs.join(rels, expandColumn === startColumn, "inner")

        val appendUdf = udf(arrayAppend _, ArrayType(LongType))
        val extendedArray = appendUdf(lhs.col(edgeListColName), relIdColumn)
        val withExtendedArray = joined.withColumn(listTempColName, extendedArray)
        val arrayContains = udf(contains _, BooleanType)(withExtendedArray.col(edgeListColName), relIdColumn)
        val filtered = withExtendedArray.filter(!arrayContains)

        // TODO: Try and get rid of the Var rel here
        val endNodeIdColNameOfJoinedRel = context.columnName(ProjectedExpr(EndNode(rel)(CTNode)))

        val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
        val withoutRelProperties = filtered.select(columns.head, columns.tail: _*)  // drops joined columns from relationship table

        withoutRelProperties
          .drop(expandColumn)
          .withColumnRenamed(endNodeIdColNameOfJoinedRel, expandColumnName)
          .drop(edgeListColName)
          .withColumnRenamed(listTempColName, edgeListColName)
      }

      private def initArray(): Any = {
        Array[Long]()
      }

      private def arrayAppend(array: Any, next: Any): Any = {
        array match {
          case a:mutable.WrappedArray[Long] =>
            a :+ next
        }
      }

      private def contains(array: Any, elem: Any): Any = {
        array match {
          case a:mutable.WrappedArray[Long] =>
            a.contains(elem)
        }
      }
    }
}
