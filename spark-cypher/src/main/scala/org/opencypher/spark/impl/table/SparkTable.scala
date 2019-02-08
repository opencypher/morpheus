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

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.ir.api.expr.{Expr, _}
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSFunctions.partitioned_id_assignment
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
      if (df.columns.toSeq == cols) {
        df
      } else {
        df.select(cols.map(df.col): _*)
      }
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

          // TODO: the join produced corrupt data when the previous operator was a cross. We work around that by using a
          // subsequent select. This can be removed, once https://issues.apache.org/jira/browse/SPARK-23855 is solved or we
          // upgrade to Spark 2.3.0
          val potentiallyCorruptedResult = df.safeJoin(other.df, joinCols, joinTypeString)
          potentiallyCorruptedResult.select("*")
      }
    }

    override def distinct: DataFrameTable = distinct(df.columns: _*)

    // workaround for https://issues.apache.org/jira/browse/SPARK-26572
    override def distinct(colNames: String*): DataFrameTable = {
      val uniqueSuffix = "_temp_distinct"

      val originalColNames = df.columns

      val renamings = originalColNames.map { c =>
        if (colNames.contains(c)) c -> s"$c$uniqueSuffix"
        else c -> c
      }.toMap

      val renamedDf = df.safeRenameColumns(colNames.map(c => c -> renamings(c)): _*)

      val extractRowFromGrouping = originalColNames.map(c => functions.first(renamings(c)) as c)
      val groupedDf = renamedDf
        .groupBy(colNames.map(c => functions.col(renamings(c))): _*)
        .agg(extractRowFromGrouping.head, extractRowFromGrouping.tail: _*)

      groupedDf.safeDropColumns(colNames.map(renamings): _*)
    }

    override def withColumnsRenamed(columnRenamings: Map[String, String]): DataFrameTable = {
      df.safeRenameColumns(columnRenamings)
    }

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

  implicit class DataFrameMeta(val df: DataFrame) extends AnyVal {
    /**
      * Returns the corresponding Cypher type for the given column name in the data frame.
      *
      * @param columnName column name
      * @return Cypher type for column
      */
    def cypherTypeForColumn(columnName: String): CypherType = {
      val structField = structFieldForColumn(columnName)
      val compatibleCypherType = structField.dataType.cypherCompatibleDataType.flatMap(_.toCypherType(structField.nullable))
      compatibleCypherType.getOrElse(
        throw IllegalArgumentException("a supported Spark DataType that can be converted to CypherType", structField.dataType))
    }

    /**
      * Returns the struct field for the given column.
      *
      * @param columnName column name
      * @return struct field
      */
    def structFieldForColumn(columnName: String): StructField = {
      if (df.schema.fieldIndex(columnName) < 0) {
        throw IllegalArgumentException(s"column with name $columnName", s"columns with names ${df.columns.mkString("[", ", ", "]")}")
      }
      df.schema.fields(df.schema.fieldIndex(columnName))
    }
  }

  implicit class DataFrameValidation(val df: DataFrame) extends AnyVal {

    def validateColumnTypes(expectedColsWithType: Map[String, CypherType]): Unit = {
      val missingColumns = expectedColsWithType.keySet -- df.schema.fieldNames.toSet

      if (missingColumns.nonEmpty) {
        throw IllegalArgumentException(
          expected = expectedColsWithType.keySet,
          actual = df.schema.fieldNames.toSet,
          s"""Expected columns are not contained in the DataFrame.
             |Missing columns: $missingColumns
           """.stripMargin
        )
      }

      val structFields = df.schema.fields.map(field => field.name -> field).toMap

      expectedColsWithType.foreach {
        case (column, cypherType) =>
          val structField = structFields(column)

          val structFieldType = structField.toCypherType match {
            case Some(cType) => cType
            case None => throw IllegalArgumentException(
              expected = s"Cypher-compatible DataType for column $column",
              actual = structField.dataType)
          }

          if (structFieldType.material.subTypeOf(cypherType.material).isFalse) {
            throw IllegalArgumentException(
              expected = s"Sub-type of $cypherType for column: $column",
              actual = structFieldType)
          }
      }
    }
  }

  implicit class DataFrameTransformation(val df: DataFrame) extends AnyVal {

    def safeAddColumn(name: String, col: Column): DataFrame = {
      require(!df.columns.contains(name),
        s"Cannot add column `$name`. A column with that name exists already. " +
          s"Use `safeReplaceColumn` if you intend to replace that column.")
      df.withColumn(name, col)
    }

    def safeAddColumns(columns: (String, Column)*): DataFrame = {
      columns.foldLeft(df) { case (tempDf, (colName, col)) =>
        tempDf.safeAddColumn(colName, col)
      }
    }

    def safeReplaceColumn(name: String, newColumn: Column): DataFrame = {
      require(df.columns.contains(name), s"Cannot replace column `$name`. No column with that name exists. " +
        s"Use `safeAddColumn` if you intend to add that column.")
      df.safeAddColumn(name, newColumn)
    }

    def safeRenameColumns(renamings: (String, String)*): DataFrame = {
      safeRenameColumns(renamings.toMap)
    }

    def safeRenameColumns(renamings: Map[String, String]): DataFrame = {
      if (renamings.isEmpty || renamings.forall { case (oldColumn, newColumn) => oldColumn == newColumn }) {
        df
      } else {
        renamings.foreach { case (oldName, newName) => require(!df.columns.contains(newName),
          s"Cannot rename column `$oldName` to `$newName`. A column with name `$newName` exists already.")
        }
        val newColumns = df.columns.map {
          case col if renamings.contains(col) => renamings(col)
          case col => col
        }
        df.toDF(newColumns: _*)
      }
    }

    def safeDropColumns(names: String*): DataFrame = {
      val nonExistentColumns = names.toSet -- df.columns
      require(nonExistentColumns.isEmpty,
        s"Cannot drop column(s) ${nonExistentColumns.map(c => s"`$c`").mkString(", ")}. They do not exist.")
      df.drop(names: _*)
    }

    def safeJoin(other: DataFrame, joinCols: Seq[(String, String)], joinType: String): DataFrame = {
      require(joinCols.map(_._1).forall(col => !other.columns.contains(col)))
      require(joinCols.map(_._2).forall(col => !df.columns.contains(col)))

      val joinExpr = joinCols.map {
        case (l, r) => df.col(l) === other.col(r)
      }.reduce((acc, expr) => acc && expr)

      df.join(other, joinExpr, joinType)
    }

    def prefixColumns(prefix: String): DataFrame =
      df.safeRenameColumns(df.columns.map(column => column -> s"$prefix$column").toMap)

    def removePrefix(prefix: String): DataFrame = {
      val columnRenamings = df.columns.collect {
        case column if column.startsWith(prefix) => column -> column.substring(prefix.length)
      }
      df.safeRenameColumns(columnRenamings.toMap)
    }

    /**
      * Cast all integer columns in a DataFrame to long.
      *
      * @return a DataFrame with all integer values cast to long
      */
    def castToLong: DataFrame = {
      def convertColumns(field: StructField, col: Column): Column = {
        val convertedCol = field.dataType match {
          case StructType(inner) =>
            val columns = inner.map(i => convertColumns(i, col.getField(i.name)).as(i.name))
            functions.struct(columns: _*)
          case ArrayType(IntegerType, nullable) => col.cast(ArrayType(LongType, nullable))
          case IntegerType => col.cast(LongType)
          case _ => col
        }
        if (col == convertedCol) col else convertedCol.as(field.name)
      }
      val convertedColumns = df.schema.fields.map { field => convertColumns(field, df.col(field.name)) }
      if (df.columns.map(df.col).sameElements(convertedColumns)) df else df.select(convertedColumns: _*)
    }

    /**
      * Adds a new column under a given name containing the hash value of the given input columns.
      *
      * The hash is generated using [[org.apache.spark.sql.catalyst.expressions.Murmur3Hash]] based on the given column
      * sequence. To decrease collision probability, we:
      *
      * 1) generate a first hash for the given column sequence
      * 2) shift the hash into the upper bits of a 64 bit long
      * 3) generate a second hash using the reversed input column sequence
      * 4) store the hash in the lower 32 bits of the final id
      *
      * @param columns    input columns for the hash function
      * @param hashColumn column storing the result of the hash function
      * @return DataFrame with an additional idColumn
      */
    def withHashColumn(columns: Seq[Column], hashColumn: String): DataFrame = {
      require(columns.nonEmpty, "Hash function requires a non-empty sequence of columns as input.")
      val tempHashValue = functions.hash(columns: _*).cast(LongType)
      val shifted = functions.shiftLeft(tempHashValue, Integer.SIZE)
      val hashValue = shifted + functions.hash(columns.reverse: _*)

      df.safeAddColumn(hashColumn, hashValue)
    }

    /**
      * Normalises the DataFrame by lifting numeric fields to Long and similar ops.
      */
    def withCypherCompatibleTypes: DataFrame = {
      val toCast = df.schema.fields.filter(f => f.toCypherType.isEmpty)
      val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(df) {
        case (currentDf, field) =>
          val castType = field.dataType.cypherCompatibleDataType.getOrElse(
            throw IllegalArgumentException(
              s"a Spark type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
              s"type ${field.dataType} of field $field"))
          currentDf.withColumn(field.name, currentDf.col(field.name).cast(castType))
      }
      dfWithCompatibleTypes
    }
  }

  implicit class DataFrameSequence(val dataFrames: Seq[DataFrame]) extends AnyVal {
    /**
      * Takes a sequence of DataFrames and adds long identifiers to all of them. Identifiers are guaranteed to be unique
      * across all given DataFrames. The DataFrames are returned in the same order as the input.
      *
      * @param idColumnName column name for the generated id
      * @return a sequence of DataFrames with unique long identifiers
      */
    def addUniqueIds(idColumnName: String): Seq[DataFrame] = {
      // We need to know how many partitions a DF has in order to avoid writing into the id space of another DF.
      // This is why require a running sum of number of partitions because we add the DF-specific sum to the offset that
      // Sparks monotonically_increasing_id adds.
      val dfPartitionCounts = dataFrames.map(_.rdd.getNumPartitions)
      val dfPartitionStartDeltas = dfPartitionCounts.scan(0)(_ + _).dropRight(1) // drop last delta, as we don't need it

      dataFrames.zip(dfPartitionStartDeltas).map {
        case (df, partitionStartDelta) =>
          df.safeAddColumn(idColumnName, partitioned_id_assignment(partitionStartDelta))
      }
    }
  }

  implicit class DataFrameDebug(val df: DataFrame) extends AnyVal {
    /**
      * Prints timing of Spark computation for DF.
      */
    def printExecutionTiming(description: String): Unit = {
      printTiming(s"$description") {
        df.count() // Force computation of DF
      }
    }

    /**
      * Prints Spark physical plan.
      */
    def printPhysicalPlan(): Unit = {
      println("Spark plan:")
      implicit val sc: SparkContext = df.sparkSession.sparkContext
      val sparkPlan: SparkPlan = df.queryExecution.executedPlan
      val planString = sparkPlan.treeString(verbose = false).flatMap {
        case '\n' => Seq('\n', '\t')
        case other => Seq(other)
      }
      println(s"\t$planString")
    }
  }
}
