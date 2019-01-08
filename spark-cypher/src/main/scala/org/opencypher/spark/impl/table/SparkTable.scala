/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.expr.{Expr, _}
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.SparkConversions._

import scala.collection.JavaConverters._

object SparkTable {

  implicit class DataFrameTable(val df: DataFrame) extends Table[DataFrameTable] {

    private case class EmptyRow()

    override def physicalColumns: Seq[String] = df.columns

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = df.toLocalIterator.asScala.map { row =>
      physicalColumns.map(c => c -> CypherValue(row.get(row.fieldIndex(c)))).toMap
    }

    override def size: Long = df.count()

    override def select(cols: String*): DataFrameTable = {
      df.select(cols.map(df.col): _*)
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      df.where(expr.asSparkSQLExpr(header, df, parameters))
    }

    override def withColumns(columns: (Expr, String)*)
      (implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      val initialColumnNameToColumn: Map[String, Column] = df.columns.map(c => c -> df.col(c)).toMap
      val updatedColumns = columns.foldLeft(initialColumnNameToColumn) { case (columnMap, (expr, columnName)) =>
        val column = expr.asSparkSQLExpr(header, df, parameters).as(columnName)
        columnMap + (columnName -> column)
      }
      // TODO: Re-enable this check as soon as types (and their nullability) are correctly inferred in typing phase
      //      if (!expr.cypherType.isNullable) {
      //        withColumn.setNonNullable(column)
      //      } else {
      //        withColumn
      //      }
      val existingColumnNames = df.columns
      // Preserve order of existing columns
      val columnsForSelect = existingColumnNames.map(updatedColumns) ++
        updatedColumns.filterKeys(!existingColumnNames.contains(_)).values

      df.select(columnsForSelect: _*)
    }

    override def drop(cols: String*): DataFrameTable = {
      df.drop(cols: _*)
    }

    override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): DataFrameTable = {
      val mappedSortItems = sortItems.map { case (expr, order) =>
        val mappedExpr = expr.asSparkSQLExpr(header, df, parameters)
        order match {
          case Ascending => mappedExpr.asc
          case Descending => mappedExpr.desc
        }
      }
      df.orderBy(mappedSortItems: _*)
    }

    override def skip(items: Long): DataFrameTable = {
      // TODO: Replace with data frame based implementation ASAP
      df.sparkSession.createDataFrame(
        df.rdd
          .zipWithIndex()
          .filter(pair => pair._2 >= items)
          .map(_._1),
        df.toDF().schema
      )
    }

    override def limit(items: Long): DataFrameTable = {
      if (items > Int.MaxValue) throw IllegalArgumentException("an integer", items)
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
      val leftTypes = df.schema.fields.flatMap(_.toCypherType)
      val rightTypes = other.df.schema.fields.flatMap(_.toCypherType)

      leftTypes.zip(rightTypes).foreach {
        case (leftType, rightType) if !leftType.nullable.couldBeSameTypeAs(rightType.nullable) =>
          throw IllegalArgumentException(
            "Equal column data types for union all (differing nullability is OK)",
            s"Left fields:  ${df.schema.fields.mkString(", ")}\n\tRight fields: ${other.df.schema.fields.mkString(", ")}")
        case _ =>
      }

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

    override def distinct: DataFrameTable = distinct(df.columns: _*)

    // workaround for https://issues.apache.org/jira/browse/SPARK-26572
    override def distinct(cols: String*): DataFrameTable = {
      val uniqueSuffix = "_temp_distinct"
      val colNames = df.columns

      val renamedDf = colNames.foldLeft(df) {
        case (df, colName) => df.safeRenameColumn(colName, s"$colName$uniqueSuffix")
      }
      val renamedColNames = renamedDf.columns.toSeq
      val extractRowFromGrouping = colNames.map(colName => functions.first(s"$colName$uniqueSuffix") as colName)
      val groupedDf = renamedDf
        .groupBy(renamedColNames.map(functions.col): _*)
        .agg(extractRowFromGrouping.head, extractRowFromGrouping.tail: _*)

      groupedDf.safeDropColumns(renamedColNames: _*)
    }

    override def withColumnRenamed(oldColumn: String, newColumn: String): DataFrameTable =
      df.safeRenameColumn(oldColumn, newColumn)

    override def cache(): DataFrameTable = {
      val planToCache = df.queryExecution.analyzed
      if (df.sparkSession.sharedState.cacheManager.lookupCachedData(planToCache).nonEmpty) {
        df.sparkSession.sharedState.cacheManager.cacheQuery(df, None, StorageLevel.MEMORY_ONLY)
      }
      this
    }

    override def show(rows: Int): Unit = df.show(rows)

    def persist(): DataFrameTable = df.persist()

    def persist(newLevel: StorageLevel): DataFrameTable = df.persist(newLevel)

    def unpersist(): DataFrameTable = df.unpersist()

    def unpersist(blocking: Boolean): DataFrameTable = df.unpersist(blocking)

    override def columnsFor(returnItem: String): Set[String] =
      throw UnsupportedOperationException("A DataFrameTable does not have return items")
  }
}
