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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.ir.api.expr.{Expr, Param}
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.Tags._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

object DataFrameOps {

  implicit class CypherRow(r: Row) {

    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: RelationalRuntimeContext[DataFrameTable]): CypherValue = {
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

  implicit class LongTagging(val l: Long) extends AnyVal {

    def setTag(tag: Int): Long = {
      (l & invertedTagMask) | (tag.toLong << idBits)
    }

    def getTag: Int = {
      // TODO: Verify that the tag actually fits into an Int or by requiring and checking a minimum size of 32 bits for idBits when reading it from config
      ((l & tagMask) >>> idBits).toInt
    }

    def replaceTag(from: Int, to: Int): Long = {
      if (l.getTag == from) l.setTag(to) else l
    }

  }

  implicit class ColumnTagging(val col: Column) extends AnyVal {

    def replaceTag(from: Int, to: Int): Column = functions
      .when(getTag === lit(from.toLong), setTag(to))
      .otherwise(col)

    def setTag(tag: Int): Column = {
      val tagLit = lit(tag.toLong << idBits)
      val newId = col
        .bitwiseAND(invertedTagMaskLit)
        .bitwiseOR(tagLit)
      newId
    }

    def getTag: Column = shiftRightUnsigned(col, idBits)
  }


  implicit class RichDataFrame(val df: DataFrame) extends AnyVal {


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

    def setNonNullable(columnName: String): DataFrame = {
      setNonNullable(Set(columnName))
    }

    def setNonNullable(columnNames: Set[String]): DataFrame = {
      val newSchema = StructType(df.schema.map {
        case s@StructField(cn, _, true, _) if columnNames.contains(cn) => s.copy(nullable = false)
        case other => other
      })
      if (newSchema == df.schema) {
        df
      } else {
        df.sparkSession.createDataFrame(df.rdd, newSchema)
      }
    }

    def setNullability(mapping: Map[String, CypherType]): DataFrame = {
      val newSchema = StructType(df.schema.map {
        case s@StructField(cn, _, _, _) => mapping.get(cn) match {
          case Some(ct) => ct.toStructField(cn)
          case None => s
        }
        case other => throw IllegalArgumentException(s"No mapping provided for $other")
      })
      if (newSchema == df.schema) {
        df
      } else {
        df.sparkSession.createDataFrame(df.rdd, newSchema)
      }
    }

    def safeReplaceTags(columnName: String, replacements: Map[Int, Int]): DataFrame = {
      val dataType = structFieldForColumn(columnName).dataType
      require(dataType == LongType, s"Cannot remap long values in Column with type $dataType")
      val col = df.col(columnName)

      val updatedCol = replacements
        .filterNot { case (from, to) => from == to }
        .foldLeft(col) {
          case (current, (from, to)) => current.replaceTag(from, to)
        }

      safeReplaceColumn(columnName, updatedCol)
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
    def printPhysicalPlan: Unit = {
      println("Spark plan:")
      implicit val sc = df.sparkSession.sparkContext
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
