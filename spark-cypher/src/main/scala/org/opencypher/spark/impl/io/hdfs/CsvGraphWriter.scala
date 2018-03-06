/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.impl.io.hdfs

import java.nio.file.Paths

import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.expr.{HasLabel, Property}
import org.opencypher.okapi.relational.impl.table.{ColumnName, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.io.hdfs.CsvGraphLoader._

class CsvGraphWriter(graph: PropertyGraph, fileHandler: CsvFileHandler)(implicit capsSession: CAPSSession) {

  val schema: Schema = graph.schema

  def store(): Unit = {
    schema.labelCombinations.combos.foreach { combo =>
      val nodes = graph.nodes("n", CTNode(combo)).asCaps
      val header = nodes.header

      val targetData = prepareDF(nodes.data, header)
      targetData.write.csv(Paths.get(fileHandler.location, NODES_DIRECTORY, fileNameFor(combo)).toString)

      writeSchemaFile(combo, header, targetData.schema)
    }
  }

  private def writeSchemaFile(labels: Set[String], header: RecordHeader, dfSchema: StructType): Unit = {
    val idFieldName = header.contents.collectFirst {
      case a: OpaqueField => ColumnName.of(a)
    }.get
    val idFieldIndex = dfSchema.fieldIndex(idFieldName)
    val idField = CsvField("CAPS_ID", idFieldIndex, "LONG")

    val propertyFields = header.contents.collect {
      case propertySlot @ ProjectedExpr(pr : Property) =>
        val index = dfSchema.fieldIndex(ColumnName.of(propertySlot))
        CsvField(pr.key.name, index, pr.cypherType)
    }

    val csvSchema = CsvNodeSchema(idField, labels.toList, List.empty, propertyFields.toList)

    fileHandler.writeSchemaFile(NODES_DIRECTORY, s"${fileNameFor(labels)}$SCHEMA_SUFFIX", csvSchema.toJson)
  }

  private def prepareDF(df: DataFrame, header: RecordHeader): DataFrame = {
    val relevantColumnNames = header.contents.filter {
      case ProjectedExpr(HasLabel(_, _)) => false
      case _ => true
    }.map(ColumnName.of)
    val withoutLabels = df.select(relevantColumnNames.head, relevantColumnNames.tail.toSeq: _*)

    df.schema.collect {
      case StructField(colName, _: ArrayType, _, _) => colName
    }.foldLeft(withoutLabels) {
      case (acc, arrayCol) =>
        val col = acc.col(arrayCol)
        acc.withColumn(arrayCol, functions.concat_ws("|", col))
    }
  }

  private def fileNameFor(labels: Set[String]): String = s"${labels.mkString("_")}$CSV_SUFFIX"
}