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
package org.opencypher.spark.api.io.fs

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource
import org.opencypher.spark.api.io.fs.HadoopFSHelpers._
import org.opencypher.spark.api.io.json.JsonSerialization

/**
  * Data source implementation that handles the writing of files and tables to a filesystem.
  *
  * By default Spark is used to write tables and the Hadoop filesystem configured in Spark is used to write files.
  * The file/folder/table structure into which the graphs are stored is defined in [[DefaultGraphDirectoryStructure]].
  *
  * @param rootPath path where the graphs are stored
  * @param tableStorageFormat Spark configuration parameter for the table format
  * @param customFileSystem optional alternative filesystem to use for writing files
  * @param filesPerTable optional parameter that specifies how many files a table is coalesced into, by default 1
  */
class FSGraphSource(
  val rootPath: String,
  val tableStorageFormat: String,
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
    caps.sparkSession.read.format(tableStorageFormat).schema(schema).load(path)
  }

  protected def writeTable(path: String, table: DataFrame): Unit = {
    val coalescedTable = filesPerTable match {
      case None => table
      case Some(numFiles) => table.coalesce(numFiles)
    }
    coalescedTable.write.format(tableStorageFormat).save(path)
  }

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphDirectory(graphName))
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
