/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.table

import java.util.Collections

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr.{Expr, _}
import org.opencypher.okapi.relational.api.table.ExtractEntities.SelectExpressionGroups
import org.opencypher.okapi.relational.api.table.FlatRelationalTable
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.SparkConversions._

import scala.collection.JavaConverters._

object SparkFlatRelationalTable {

  implicit class DataFrameTable(val df: DataFrame) extends FlatRelationalTable[DataFrameTable] {

    override def empty(initialHeader: RecordHeader = RecordHeader.empty): DataFrameTable = {
      df.sparkSession.createDataFrame(Collections.emptyList[Row](), initialHeader.toStructType)
    }

    private case class EmptyRow()

    override def unit: DataFrameTable = {
      df.sparkSession.createDataFrame(Seq(EmptyRow()))
    }

    override def physicalColumns: Seq[String] = df.columns

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = df.toLocalIterator.asScala.map { row =>
      physicalColumns.map(c => c -> CypherValue(row.get(row.fieldIndex(c)))).toMap
    }

    override def size: Long = df.count()

    override def select(cols: String*): DataFrameTable = {
      if (cols.nonEmpty) {
        df.select(cols.head, cols.tail: _*)
      } else {
        // TODO: this is used in Construct, check why this is necessary
        df.select()
      }
    }

    override def extractEntities(
      selectGroups: SelectExpressionGroups,
      baseTableHeader: RecordHeader,
      targetHeader: RecordHeader
    )(implicit parameters: CypherMap): DataFrameTable = {
      val missingColumns = selectGroups.flatMap { group =>
        group.filter {
          case (_, targetColumnName) => !df.columns.contains(targetColumnName)
        }
      }
      val tableWithMissingColumns = missingColumns.foldLeft(this) {
        case (currentDf, (expr, targetColumnName)) =>
          currentDf.withColumn(targetColumnName, expr, preserveNullability = false)(baseTableHeader, parameters)
      }

      val idColumn = targetHeader.column(targetHeader.entityVars.head)
      val encoder = targetHeader.rowEncoder
      val idIndex = encoder.schema.fieldIndex(idColumn)
      val orderedSelectGroups = selectGroups.map { selectGroup => selectGroup.sortBy(_._2) }

      val entityTable = tableWithMissingColumns.df.flatMap { row =>
        orderedSelectGroups.map { selectGroup =>
          Row.fromSeq(selectGroup.map { case (_, column) => row.get(row.fieldIndex(column))})
        }.filterNot(_.isNullAt(idIndex))
      }(encoder)

      entityTable.setNullability(targetHeader.exprToColumn.map { case (expr, column) => column -> expr.cypherType })
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      df.where(expr.asSparkSQLExpr(header, df, parameters))
    }

    // TODO: correctly set cypher type on call site instead of boolean flag
    override def withColumn(column: String, expr: Expr, preserveNullability: Boolean = true)
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      val withColumn = df.withColumn(column, expr.asSparkSQLExpr(header, df, parameters))

      if (preserveNullability && !expr.cypherType.isNullable) {
        withColumn.setNonNullable(column)
      } else {
        withColumn
      }
    }

    override def drop(cols: String*): DataFrameTable = {
      df.drop(cols: _*)
    }

    override def orderBy(sortItems: (String, Order)*): DataFrameTable = {
      val sortExpression = sortItems.map {
        case (column, Ascending) => asc(column)
        case (column, Descending) => desc(column)
      }

      df.sort(sortExpression: _*)
    }

    override def skip(items: Long): DataFrameTable = {
      // TODO: Replace with data frame based implementation ASAP
      df.sparkSession.createDataFrame(
        df.toDF().rdd
          .zipWithIndex()
          .filter(pair => pair._2 >= items)
          .map(_._1),
        df.toDF().schema
      )
    }

    override def limit(items: Long): DataFrameTable = {
      // TODO: Unsafe `toInt`
      df.limit(items.toInt)
    }

    override def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {

      def withInnerExpr(expr: Expr)(f: Column => Column) =
        f(expr.asSparkSQLExpr(header, df, parameters))

      val data: Either[RelationalGroupedDataset, DataFrame] =
        if (by.nonEmpty) {
          val columns = by.flatMap { expr =>
            val withChildren = header.ownedBy(expr)
            withChildren.map(e => withInnerExpr(e)(identity))
          }
          Left(df.groupBy(columns.toSeq: _*))
        } else {
          Right(df)
        }

      val sparkAggFunctions = aggregations.map {
        case (aggFunc, (columnName, cypherType)) =>
          aggFunc match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                functions
                  .avg(_)
                  .cast(cypherType.getSparkType)
                  .as(columnName))

            case CountStar(_) =>
              functions.count(functions.lit(0)).as(columnName)

            // TODO: Consider not implicitly projecting the aggFunc expr here, but rewriting it into a variable in logical planning or IR construction
            case Count(expr, distinct) => withInnerExpr(expr) { column =>
              val count = {
                if (distinct) functions.countDistinct(column)
                else functions.count(column)
              }
              count.as(columnName)
            }

            case Max(expr) =>
              withInnerExpr(expr)(functions.max(_).as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(functions.min(_).as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(functions.sum(_).as(columnName))

            case Collect(expr, distinct) => withInnerExpr(expr) { column =>
              val list = {
                if (distinct) functions.collect_set(column)
                else functions.collect_list(column)
              }
              // sort for deterministic aggregation results
              val sorted = functions.sort_array(list)
              sorted.as(columnName)
            }

            case x =>
              throw NotImplementedException(s"Aggregation function $x")
          }
      }

      data.fold(
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*),
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*)
      )
    }

    override def unionAll(other: DataFrameTable): DataFrameTable = {
      df.union(other.df)
    }

    override def join(other: DataFrameTable, joinType: JoinType, joinCols: (String, String)*): DataFrameTable = {
      val joinTypeString = joinType match {
        case InnerJoin => "inner"
        case LeftOuterJoin => "left_outer"
        case RightOuterJoin => "right_outer"
        case FullOuterJoin => "full_outer"
        case CrossJoin => "cross"
      }

      joinType match {
        case CrossJoin =>
          df.crossJoin(other.df)

        case _ =>
          val joinExpr = joinCols.map {
            case (l, r) => df.col(l) === other.df.col(r)
          }.reduce((acc, expr) => acc && expr)

          // TODO: the join produced corrupt data when the previous operator was a cross. We work around that by using a
          // subsequent select. This can be removed, once https://issues.apache.org/jira/browse/SPARK-23855 is solved or we
          // upgrade to Spark 2.3.0
          val potentiallyCorruptedResult = df.join(other.df, joinExpr, joinTypeString)
          potentiallyCorruptedResult.select("*")
      }
    }

    override def distinct: DataFrameTable =
      df.distinct

    override def distinct(cols: String*): DataFrameTable =
      df.dropDuplicates(cols)

    override def withColumnRenamed(oldColumn: String, newColumn: String): DataFrameTable =
      df.safeRenameColumn(oldColumn, newColumn)

    override def cache(): DataFrameTable = df.cache()

    override def show(rows: Int): Unit = df.show(rows)

    def persist(): DataFrameTable = df.persist()

    def persist(newLevel: StorageLevel): DataFrameTable = df.persist(newLevel)

    def unpersist(): DataFrameTable = df.unpersist()

    def unpersist(blocking: Boolean): DataFrameTable = df.unpersist(blocking)
  }
}
