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
package org.opencypher.spark.api.io.fs

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, NullType, StringType, StructType}
import org.opencypher.okapi.api.graph.{GraphName, Node, Relationship}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure._
import org.opencypher.spark.api.io.fs.HadoopFSHelpers._
import org.opencypher.spark.api.io.json.JsonSerialization
import org.opencypher.spark.api.io.util.HiveTableName
import org.opencypher.spark.api.io.{AbstractPropertyGraphDataSource, FileFormat, StorageFormat}
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkTable._

/**
  * Data source implementation that handles the writing of files and tables to a filesystem.
  *
  * By default Spark is used to write tables and the Hadoop filesystem configured in Spark is used to write files.
  * The file/folder/table structure into which the graphs are stored is defined in [[DefaultGraphDirectoryStructure]].
  *
  * @param rootPath path where the graphs are stored
  * @param tableStorageFormat Spark configuration parameter for the table format
  * @param hiveDatabaseName optional Hive database to write tables to
  * @param filesPerTable optional parameter that specifies how many files a table is coalesced into, by default 1
  */
class FSGraphSource(
  val rootPath: String,
  val tableStorageFormat: StorageFormat,
  val hiveDatabaseName: Option[String] = None,
  val filesPerTable: Option[Int] = None
)(override implicit val caps: CAPSSession)
  extends AbstractPropertyGraphDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  protected lazy val fileSystem: FileSystem = {
    FileSystem.get(new URI(rootPath), caps.sparkSession.sparkContext.hadoopConfiguration)
  }

  protected def listDirectories(path: String): List[String] = fileSystem.listDirectories(path)

  protected def deleteDirectory(path: String): Unit = fileSystem.deleteDirectory(path)

  protected def readFile(path: String): String = fileSystem.readFile(path)

  protected def writeFile(path: String, content: String): Unit = fileSystem.writeFile(path, content)

  protected def readTable(path: String, schema: StructType): DataFrame = {
    caps.sparkSession.read.format(tableStorageFormat.name).schema(schema).load(path)
  }

  protected def writeTable(path: String, table: DataFrame): Unit = {
    val coalescedTable = filesPerTable match {
      case None => table
      case Some(numFiles) => table.coalesce(numFiles)
    }
    // TODO: Consider changing computation of canonical tables so `null` typed columns don't make it here
    // Null typed columns cannot be stored by most FS formats
    val h :: t = coalescedTable.columns.collect {
      case c if coalescedTable.schema(c).dataType != NullType => c
    }.toList
    coalescedTable.select(h, t: _*).write.format(tableStorageFormat.name).save(path)
  }

  override protected def listGraphNames: List[String] = {
    def traverse(parent: String): List[String] = {
      val children = listDirectories(parent)
      if (children.contains(nodeTablesDirectoryName) || children.contains(relationshipTablesDirectoryName)) {
        List(parent)
      } else {
        children.flatMap(child => traverse(parent / child))
      }
    }

    traverse(rootPath)
      .map(_.substring(rootPath.length))
      .map(_.replace(pathSeparator, "."))
      .map(_.substring(1))
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphDirectory(graphName))
    if (hiveDatabaseName.isDefined) {
      deleteHiveDatabase(graphName)
    }
  }

  override protected def readNodeTable(
    graphName: GraphName,
    labels: Set[String],
    sparkSchema: StructType
  ): DataFrame = {
    readTable(pathToNodeTable(graphName, labels), sparkSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = {
    writeTable(pathToNodeTable(graphName, labels), table)
    if (hiveDatabaseName.isDefined) {
      val hiveNodeTableName = HiveTableName(hiveDatabaseName.get, graphName, Node, labels)
      writeHiveTable(pathToNodeTable(graphName, labels), hiveNodeTableName, table.schema)
    }
  }

  override protected def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = {
    readTable(pathToRelationshipTable(graphName, relKey), sparkSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), table)
    if (hiveDatabaseName.isDefined) {
      val hiveRelationshipTableName = HiveTableName(hiveDatabaseName.get, graphName, Relationship, Set(relKey))
      writeHiveTable(pathToRelationshipTable(graphName, relKey), hiveRelationshipTableName, table.schema)
    }
  }

  private def writeHiveTable(pathToTable: String, hiveTableName: String, schema: StructType): Unit = {
    caps.sparkSession.catalog.createTable(hiveTableName, tableStorageFormat.name, schema, Map("path" -> pathToTable))
    caps.sparkSession.catalog.refreshTable(hiveTableName)
  }

  private def deleteHiveDatabase(graphName: GraphName): Unit = {
    val graphSchema = schema(graphName).get
    val labelCombinations = graphSchema.labelCombinations.combos
    val relTypes = graphSchema.relationshipTypes

    labelCombinations.foreach { combo =>
      val tableName = HiveTableName(hiveDatabaseName.get, graphName, Node, combo)
      caps.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    relTypes.foreach { relType =>
      val tableName = HiveTableName(hiveDatabaseName.get, graphName, Relationship, Set(relType))
      caps.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  override protected def readJsonSchema(graphName: GraphName): String = {
    readFile(pathToGraphSchema(graphName))
  }

  override protected def writeJsonSchema(graphName: GraphName, schema: String): Unit = {
    writeFile(pathToGraphSchema(graphName), schema)
  }

  override protected def readJsonCAPSGraphMetaData(graphName: GraphName): String = {
    readFile(pathToCAPSMetaData(graphName))
  }

  override protected def writeJsonCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: String): Unit = {
    writeFile(pathToCAPSMetaData(graphName), capsGraphMetaData)
  }

}

/**
  * Spark CSV does not support storing BinaryType columns by default. This data source implementation encodes BinaryType
  * columns to Hex-encoded strings and decodes such columns back to BinaryType. This feature is required because ids
  * within CAPS are stored as BinaryType.
  */
class CsvGraphSource(rootPath: String, filesPerTable: Option[Int] = None)(override implicit val caps: CAPSSession)
  extends FSGraphSource(rootPath, FileFormat.csv, None, filesPerTable) {

  override protected def writeTable(path: String, table: DataFrame): Unit =
    super.writeTable(path, table.encodeBinaryToHexString)

  protected override def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame =
    readEntityTable(graphName, Left(labels), sparkSchema)

  protected override def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = readEntityTable(graphName, Right(relKey), sparkSchema)

  private def readEntityTable(
    graphName: GraphName,
    labelsOrRelKey: Either[Set[String], String],
    sparkSchema: StructType
  ): DataFrame = {
    val readSchema = sparkSchema.convertTypes(BinaryType, StringType)

    val tableWithEncodedStrings = labelsOrRelKey match {
      case Left(labels) => super.readNodeTable(graphName, labels, readSchema)
      case Right(relKey) => super.readRelationshipTable(graphName, relKey, readSchema)
    }

    tableWithEncodedStrings.decodeHexStringToBinary(sparkSchema.binaryColumns)
  }
}
