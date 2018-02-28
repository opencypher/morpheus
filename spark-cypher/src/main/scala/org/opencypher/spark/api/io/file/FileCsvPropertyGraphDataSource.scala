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
package org.opencypher.spark.api.io.file

import java.io.File
import java.nio.file.{Files, Paths}

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.io.hdfs.CsvGraphLoader

import scala.collection.JavaConverters._

/**
  * Loads a graph stored in indexed CSV format from the local file system.
  * The CSV files must be stored following this schema:
  *
  * # Nodes
  *   - all files describing nodes are stored in a sub folder called "nodes"
  *   - create one file for each possible label combination that exists in the data. This means that a node can only
  *     be present in one file. Example: All nodes with labels :Person:Employee are in a single file and all nodes that
  *     have label :Person are stored in another file. A node that appears in :Person:Employee CANNOT appear again in the
  *     file for :Person.
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[org.opencypher.spark.impl.io.hdfs.CsvNodeSchema]]
  *
  * # Relationships
  *   - all files describing relationships are stored in a sub folder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[org.opencypher.spark.impl.io.hdfs.CsvRelSchema]]
  *
  * @param graphFolder path to the folder containing the nodes/relationships folders
  * @param session CAPS Session
  */
case class FileCsvPropertyGraphDataSource(graphFolder: String)(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = CsvGraphLoader(graphPath(name)).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw new UnsupportedOperationException("'store' operation is not supported by the FileCSV data source")

  override def delete(name: GraphName): Unit =
    if (hasGraph(name)) Files.delete(Paths.get(graphPath(name)))

  override def graphNames: Set[GraphName] = Files.list(Paths.get(graphFolder)).iterator().asScala
    .filter(p => Files.isDirectory(p))
    .map(p => p.getFileName.toString)
    .map(GraphName)
    .toSet

  override def hasGraph(name: GraphName): Boolean = Files.exists(Paths.get(graphPath(name)))

  private def graphPath(name: GraphName): String = s"$graphFolder${File.separator}$name"
}
