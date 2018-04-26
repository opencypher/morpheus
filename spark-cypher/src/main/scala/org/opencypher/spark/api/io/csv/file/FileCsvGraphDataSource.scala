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
package org.opencypher.spark.api.io.csv.file

import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.http.client.utils.URIBuilder
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.io.hdfs.{CsvGraphLoader, CsvGraphWriter}

import scala.collection.JavaConverters._

/**
  * Loads a graph stored in CSV format from the local file system.
  * The CSV files must be stored following this schema:
  *
  * # Nodes
  *   - all files describing nodes are stored in a sub folder called "nodes"
  *   - create one file for each possible label combination that exists in the data. This means that a node can only
  * be present in one file. Example: All nodes with labels :Person:Employee are in a single file and all nodes that
  * have label :Person are stored in another file. A node that appears in :Person:Employee CANNOT appear again in the
  * file for :Person.
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[org.opencypher.spark.impl.io.hdfs.CsvNodeSchema]]
  *
  * # Relationships
  *   - all files describing relationships are stored in a sub folder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[org.opencypher.spark.impl.io.hdfs.CsvRelSchema]]
  *
  * @param rootPath path to the folder containing the nodes/relationships folders
  * @param session  CAPS Session
  */
case class FileCsvGraphDataSource(rootPath: String)(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = CsvGraphLoader(graphPath(name)).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    CsvGraphWriter(graph, graphPath(name)).store()

  override def delete(name: GraphName): Unit =
    if (hasGraph(name)) FileUtils.deleteDirectory(Paths.get(graphPath(name)).toFile)

  override def graphNames: Set[GraphName] = Files.list(Paths.get(rootPath)).iterator().asScala
    .filter(p => Files.isDirectory(p))
    .map(p => p.getFileName.toString)
    .map(GraphName)
    .toSet

  override def hasGraph(name: GraphName): Boolean = Files.exists(Paths.get(graphPath(name)))

  private def graphPath(name: GraphName): URI =
    new URIBuilder(rootPath)
      .setScheme("file")
      .setPath(Paths.get(rootPath, name.value).toString).build()
}
