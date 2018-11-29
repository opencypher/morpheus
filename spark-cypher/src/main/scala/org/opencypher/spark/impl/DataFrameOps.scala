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
package org.opencypher.spark.impl

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSFunctions.partitioned_id_assignment
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

object DataFrameOps {

  /**
    * Takes a sequence of DataFrames and adds long identifiers to all of them. Identifiers are guaranteed to be unique
    * across all given DataFrames. The DataFrames are returned in the same order as the input.
    *
    * @param dataFrames   sequence of DataFrames to assign ids to
    * @param idColumnName column name for the generated id
    * @return a sequence of DataFrames with unique long identifiers
    */
  def addUniqueIds(dataFrames: Seq[DataFrame], idColumnName: String): Seq[DataFrame] = {
    // We need to know how many partitions a DF has in order to avoid writing into the id space of another DF.
    // This is why require a running sum of number of partitions because we add the DF-specific sum to the offset that
    // Sparks monotonically_increasing_id adds.
    val dfPartitionCounts = dataFrames.map(_.rdd.getNumPartitions)
    val dfPartitionStartDeltas = dfPartitionCounts.scan(0)(_ + _).dropRight(1) // drop last delta, as we don't need it

    dataFrames.zip(dfPartitionStartDeltas).map {
      case (df, partitionStartDelta) =>
        df.withColumn(idColumnName, partitioned_id_assignment(partitionStartDelta))
    }
  }

  implicit class CypherRow(r: Row) {

    def getCypherValue(expr: Expr, header: RecordHeader)
      (implicit context: RelationalRuntimeContext[DataFrameTable]): CypherValue = {
      expr match {
        case Param(name) => context.parameters(name)
        case _ =>
          header.getColumn(expr) match {
            case None => throw IllegalArgumentException(s"column for $expr")
            case Some(column) => CypherValue(r.get(r.schema.fieldIndex(column)))
          }
      }
    }
  }

  implicit class RichDataFrame(val df: DataFrame) extends AnyVal {

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

    def mapColumn(name: String)(f: Column => Column): DataFrame = {
      df.withColumn(name, f(df.col(name)))
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
      * @param columns input columns for the hash function
      * @param idColumn column storing the result of the hash function
      * @return DataFrame with an additional idColumn
      */
    def withHashColumn(columns: Seq[Column], idColumn: String): DataFrame = {
      require(columns.nonEmpty, "Hash function requires a non-empty sequence of columns as input.")
      val id1 = functions.hash(columns: _*).cast(LongType)
      val shifted = functions.shiftLeft(id1, Integer.SIZE)
      val id = shifted + functions.hash(columns.reverse: _*)

      df.withColumn(idColumn, id)
    }

    def withPropertyColumns: DataFrame = {
      df.columns.foldLeft(df) {
        case (currentDf, column) => currentDf.withColumnRenamed(column, column.toPropertyColumnName)
      }
    }

    def prefixColumns(prefix: String): DataFrame = {
      df.columns.foldLeft(df) {
        case (currentDf, column) => currentDf.withColumnRenamed(column, s"$prefix$column")
      }
    }

    def removePrefix(prefix: String): DataFrame = {
      df.columns.foldLeft(df) {
        case (currentDf, column) if column.startsWith(prefix) => currentDf.withColumnRenamed(column, column.substring(prefix.length))
        case (currentDf, _) => currentDf
      }
    }

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
      df.withColumn(name, newColumn)
    }

    def safeRenameColumn(oldName: String, newName: String): DataFrame = {
      require(!df.columns.contains(newName),
        s"Cannot rename column `$oldName` to `$newName`. A column with name `$newName` exists already.")
      df.withColumnRenamed(oldName, newName)
    }

    def safeRenameColumns(renamings: (String, String)*): DataFrame = {
      renamings.foldLeft(df) { case (tempDf, (oldName, newName)) =>
        tempDf.safeRenameColumn(oldName, newName)
      }
    }

    def safeDropColumn(name: String): DataFrame = {
      require(df.columns.contains(name),
        s"Cannot drop column `$name`. No column with that name exists.")
      df.drop(name)
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

    /**
      * Normalises the dataframe by lifting numeric fields to Long and similar ops.
      */
    def withCypherCompatibleTypes: DataFrame = {
      val toCast = df.schema.fields.filter(f => f.toCypherType.isEmpty)
      val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(df) {
        case (currentDf, field) =>
          val castType = field.dataType.cypherCompatibleDataType.getOrElse(
            throw IllegalArgumentException(
              s"a Spark type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
              s"type ${field.dataType} of field $field"))
          currentDf.mapColumn(field.name)(_.cast(castType))
      }
      dfWithCompatibleTypes
    }

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

    /**
      * Caches and forces the computation of the DF
      *
      * @return row count of the DF
      */
    def cacheAndForce(tableName: Option[String] = None): Long = {
      df.sparkSession.sharedState.cacheManager.cacheQuery(df, tableName, MEMORY_ONLY)
      val rowCount = df.count() // Force computation of cached DF
      assert(df.storageLevel.useMemory)
      rowCount
    }

  }

}
