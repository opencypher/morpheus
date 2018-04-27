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
package org.opencypher.spark.impl.io.hdfs

import java.net.URI
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
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
    writeMetaData()
  }

  private def writeMetaData(): Unit =
    fileHandler.writeMetaData(METADATA_FILE, CsvGraphMetaData(graph.asCaps.tags))

  private def writeNodes(): Unit = {
    schema.labelCombinations.combos.foreach { labelCombination =>
      val nodes = graph.asCaps.nodesWithExactLabels("n", labelCombination)
      val header = nodes.header

      val targetData = prepareDataFrame(nodes.data, header)
      val path = Paths.get(fileHandler.graphLocation.getPath, NODES_DIRECTORY, fileNameFor(labelCombination)).toString
      targetData.write.csv(path)

      writeNodeSchema(labelCombination, header, targetData.schema)
    }
  }

  private def writeNodeSchema(labels: Set[String], header: RecordHeader, dfSchema: StructType): Unit = {
    val idFieldName = header.contents.collectFirst {
      case a: OpaqueField => ColumnName.of(a)
    }.get
    val idField = CsvField("id", dfSchema.fieldIndex(idFieldName), "LONG", Some(false))

    val csvSchema = CsvNodeSchema(idField, labels.toList, List.empty, getPropertySchema(header, dfSchema).toList)

    fileHandler.writeSchemaFile(NODES_DIRECTORY, s"${fileNameFor(labels)}$SCHEMA_SUFFIX", csvSchema.toJson)
  }

  private def writeRelationships(): Unit = {
    schema.relationshipTypes.foreach { relType =>
      val rels = graph.relationships("r", CTRelationship(relType)).asCaps
      val header = rels.header

      val targetData = prepareDataFrame(rels.data, header)
      val path = Paths.get(fileHandler.graphLocation.getPath, RELS_DIRECTORY, fileNameFor(relType)).toString
      targetData.write.csv(path)

      writeRelationshipSchema(relType, header, targetData.schema)
    }
  }

  private def writeRelationshipSchema(relType: String, header: RecordHeader, dfSchema: StructType): Unit = {
    val idFieldName = header.contents.collectFirst {
      case a: OpaqueField => ColumnName.of(a)
    }.get
    val idField = CsvField("id", dfSchema.fieldIndex(idFieldName), "LONG", Some(false))

    val startIdFieldName = header.contents.collectFirst {
      case a@ProjectedExpr(StartNode(_)) => ColumnName.of(a)
    }.get
    val startIdField = CsvField("startId", dfSchema.fieldIndex(startIdFieldName), "LONG", Some(false))

    val endIdFieldName = header.contents.collectFirst {
      case a@ProjectedExpr(EndNode(_)) => ColumnName.of(a)
    }.get
    val endIdField = CsvField("endId", dfSchema.fieldIndex(endIdFieldName), "LONG", Some(false))

    val csvSchema = CsvRelSchema(idField, startIdField, endIdField, relType, getPropertySchema(header, dfSchema).toList)

    fileHandler.writeSchemaFile(RELS_DIRECTORY, s"${fileNameFor(relType)}$SCHEMA_SUFFIX", csvSchema.toJson)
  }

  private def getPropertySchema(header: RecordHeader, dfSchema: StructType) = {
    val propertyFields = header.contents.collect {
      case propertySlot@ProjectedExpr(pr: Property) =>
        val index = dfSchema.fieldIndex(ColumnName.of(propertySlot))
        CsvField(pr.key.name, index, pr.cypherType, dfSchema.fields(index).nullable)
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

object CsvGraphWriter {
  def apply(graph: PropertyGraph, location: URI, hadoopConfig: Configuration)(implicit caps: CAPSSession): CsvGraphWriter = {
    new CsvGraphWriter(graph, new HadoopFileHandler(location, hadoopConfig))
  }

  def apply(graph: PropertyGraph, location: URI)(implicit caps: CAPSSession): CsvGraphWriter = {
    new CsvGraphWriter(graph, new LocalFileHandler(location))
  }
}