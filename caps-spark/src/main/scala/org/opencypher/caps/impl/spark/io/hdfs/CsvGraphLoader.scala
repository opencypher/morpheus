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
package org.opencypher.caps.impl.spark.io.hdfs

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}

trait CsvGraphLoaderFileHandler {
  def location: String
  def listDataFiles(directory: String): Array[URI]
  def readSchemaFile(path: URI): String
}

final class HadoopFileHandler(override val location: String, private val hadoopConfig: Configuration)
    extends CsvGraphLoaderFileHandler {

  private val fs: FileSystem = FileSystem.get(new URI(location), hadoopConfig)

  override def listDataFiles(directory: String): Array[URI] = {
    fs.listStatus(new Path(s"$location${File.separator}$directory"))
      .filter(p => p.getPath.toString.endsWith(".csv") | p.getPath.toString.endsWith(".CSV"))
      .map(_.getPath.toUri)
  }

  override def readSchemaFile(path: URI): String = {
    val hdfsPath = new Path(path)
    val schemaPaths = Seq(hdfsPath.suffix(".schema"), hdfsPath.suffix(".SCHEMA"))
    val optSchemaPath = schemaPaths.find(fs.exists)
    val schemaPath = optSchemaPath.getOrElse(throw IllegalArgumentException(s"to find a schema file at $path"))
    val stream = new BufferedReader(new InputStreamReader(fs.open(schemaPath)))
    def readLines = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    readLines.takeWhile(_ != null).mkString
  }
}

final class LocalFileHandler(override val location: String) extends CsvGraphLoaderFileHandler {
  import scala.collection.JavaConverters._

  override def listDataFiles(directory: String): Array[URI] = {
    Files
      .list(Paths.get(s"$location${File.separator}$directory"))
      .collect(Collectors.toList())
      .asScala
      .filter(p => p.toString.endsWith(".csv") | p.toString.endsWith(".CSV"))
      .toArray
      .map(_.toUri)
  }

  override def readSchemaFile(csvPath: URI): String = {
    val schemaPaths = Seq(
      new URI(s"${csvPath.toString}.schema"),
      new URI(s"${csvPath.toString}.SCHEMA")
    )

    val optSchemaPath = schemaPaths.find(p => new File(p).exists())
    val schemaPath = optSchemaPath.getOrElse(throw IllegalArgumentException(s"Could not find schema file at $csvPath"))
    new String(Files.readAllBytes(Paths.get(schemaPath)))
  }
}

/**
  * Loads a graph stored in indexed CSV format from HDFS or the local file system
  * The CSV files must be stored following this schema:
  * # Nodes
  *   - all files describing nodes are stored in a subfolder called "nodes"
  *   - create one file for each possible label combination that exists in the data, i.e. there must not be overlapping
  *     entities in different files (e.g. all nodes with labels :Person:Employee in a single file and all nodes that
  *     have label :Person exclusively in another file)
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[CsvNodeSchema]]
  * # Relationships
  *   - all files describing relationships are stored in a subfolder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[CsvRelSchema]]

  * @param fileHandler CsvGraphLoaderFileHandler file handler for hdfs or local file system
  * @param capsSession CAPS Session
  */
class CsvGraphLoader(fileHandler: CsvGraphLoaderFileHandler)(implicit capsSession: CAPSSession) {

  private val sparkSession: SparkSession = capsSession.sparkSession

  def load: CAPSGraph = {
    val nodeScans = loadNodes
    val relScans = loadRels
    CAPSGraph.create(nodeScans.head, nodeScans.tail ++ relScans: _*)
  }

  private def loadNodes: Array[NodeScan] = {
    val csvFiles = listCsvFiles("nodes")

    csvFiles.map(e => {
      val schema = parseSchema(e)(CsvNodeSchema(_))

      val records = CAPSRecords.create(
        sparkSession.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .schema(schema.toStructType)
          .csv(e.toString)
      )

      NodeScan
        .on("n" -> schema.idField.name)(builder => {
          val withImpliedLabels = schema.implicitLabels.foldLeft(builder.build)(_ withImpliedLabel _)
          val withOptionalLabels = schema.optionalLabels.foldLeft(withImpliedLabels)((a, b) => {
            a.withOptionalLabel(b.name -> b.name)
          })
          schema.propertyFields.foldLeft(withOptionalLabels)((builder, field) => {
            builder.withPropertyKey(field.name -> field.name)
          })
        })
        .from(records)
    })
  }

  private def loadRels: Array[RelationshipScan] = {
    val csvFiles = listCsvFiles("relationships")

    csvFiles.map(relationShipFile => {

      val schema = parseSchema(relationShipFile)(CsvRelSchema(_))

      val records = CAPSRecords.create(
        sparkSession.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .schema(schema.toStructType)
          .csv(relationShipFile.toString)
      )

      RelationshipScan
        .on("r" -> schema.idField.name)(builder => {
          val baseBuilder = builder
            .from(schema.startIdField.name)
            .to(schema.endIdField.name)
            .relType(schema.relType)
            .build

          schema.propertyFields.foldLeft(baseBuilder)((builder, field) => {
            builder.withPropertyKey(field.name -> field.name)
          })
        })
        .from(records)
    })
  }

  private def listCsvFiles(directory: String): Array[URI] =
    fileHandler.listDataFiles(directory)

  private def parseSchema[T <: CsvSchema](path: URI)(parser: String => T): T = {
    val text = fileHandler.readSchemaFile(path)
    parser(text)
  }
}

object CsvGraphLoader {
  def apply(location: String, hadoopConfig: Configuration)(implicit caps: CAPSSession): CsvGraphLoader = {
    new CsvGraphLoader(new HadoopFileHandler(location, hadoopConfig))
  }

  def apply(location: String)(implicit caps: CAPSSession): CsvGraphLoader = {
    new CsvGraphLoader(new LocalFileHandler(location))
  }
}
