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
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.io.hdfs.CsvGraphLoader._

class CsvGraphWriter(graph: PropertyGraph, fileHandler: CsvFileHandler)(implicit capsSession: CAPSSession) {

  val schema: Schema = graph.schema

  def store(): Unit = {
    writeNodes()
    writeRelationships()
  }

  private def writeNodes(): Unit = {
    schema.labelCombinations.combos.foreach { labelCombination =>
      val nodes = graph.asCaps.nodesWithExactLabels("n", labelCombination)
      val header = nodes.header

      val targetData = prepareDataFrame(nodes.data, header)
      targetData.write.csv(Paths.get(fileHandler.location, NODES_DIRECTORY, fileNameFor(labelCombination)).toString)

      writeNodeSchema(labelCombination, header, targetData.schema)
    }
  }

  private def writeNodeSchema(labels: Set[String], header: RecordHeader, dfSchema: StructType): Unit = {
    val idFieldName = header.contents.collectFirst {
      case a: OpaqueField => ColumnName.of(a)
    }.get
    val idField = CsvField("id", dfSchema.fieldIndex(idFieldName), "LONG")

    val csvSchema = CsvNodeSchema(idField, labels.toList, List.empty, getPropertySchema(header, dfSchema).toList)

    fileHandler.writeSchemaFile(NODES_DIRECTORY, s"${fileNameFor(labels)}$SCHEMA_SUFFIX", csvSchema.toJson)
  }

  private def writeRelationships(): Unit = {
    schema.relationshipTypes.foreach { relType =>
      val rels = graph.relationships("r", CTRelationship(relType)).asCaps
      val header = rels.header

      val targetData = prepareDataFrame(rels.data, header)
      targetData.write.csv(Paths.get(fileHandler.location, RELS_DIRECTORY, fileNameFor(relType)).toString)

      writeRelationshipSchema(relType, header, targetData.schema)
    }
  }

  private def writeRelationshipSchema(relType: String, header: RecordHeader, dfSchema: StructType): Unit = {
    val idFieldName = header.contents.collectFirst {
      case a: OpaqueField => ColumnName.of(a)
    }.get
    val idField = CsvField("id", dfSchema.fieldIndex(idFieldName), "LONG")

    val startIdFieldName = header.contents.collectFirst {
      case a@ProjectedExpr(StartNode(_)) => ColumnName.of(a)
    }.get
    val startIdField = CsvField("startId", dfSchema.fieldIndex(startIdFieldName), "LONG")

    val endIdFieldName = header.contents.collectFirst {
      case a@ProjectedExpr(EndNode(_)) => ColumnName.of(a)
    }.get
    val endIdField = CsvField("endId", dfSchema.fieldIndex(endIdFieldName), "LONG")

    val csvSchema = CsvRelSchema(idField, startIdField, endIdField, relType, getPropertySchema(header, dfSchema).toList)

    fileHandler.writeSchemaFile(RELS_DIRECTORY, s"${fileNameFor(relType)}$SCHEMA_SUFFIX", csvSchema.toJson)
  }

  private def getPropertySchema(header: RecordHeader, dfSchema: StructType) = {
    val propertyFields = header.contents.collect {
      case propertySlot@ProjectedExpr(pr: Property) =>
        val index = dfSchema.fieldIndex(ColumnName.of(propertySlot))
        CsvField(pr.key.name, index, pr.cypherType)
    }
    propertyFields
  }

  /**
    * Removes label and and relationship type columns from a node data frame or a relationships data frame, respectively.
    *
    * Converts values of array columns into a concatenated string.
    *
    * @param df     node or relationship dataframe
    * @param header record header
    * @return df without label or type column
    */
  private def prepareDataFrame(df: DataFrame, header: RecordHeader): DataFrame = {
    val relevantColumnNames = header.contents.filter {
      case ProjectedExpr(HasLabel(_, _)) => false
      case ProjectedExpr(Type(_)) => false
      case _ => true
    }.map(ColumnName.of)
    val trimmedDF = df.select(relevantColumnNames.head, relevantColumnNames.tail.toSeq: _*)

    // concat array fields
    df.schema.collect {
      case StructField(colName, _: ArrayType, _, _) => colName
    }.foldLeft(trimmedDF) {
      case (acc, arrayCol) =>
        val col = acc.col(arrayCol)
        acc.withColumn(arrayCol, functions.concat_ws("|", col))
    }
  }

  private def fileNameFor(relTypeOrLabel: String): String = fileNameFor(Set(relTypeOrLabel))

  private def fileNameFor(labels: Set[String]): String = s"${labels.mkString("_")}$CSV_SUFFIX"
}