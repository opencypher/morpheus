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
package org.opencypher.spark.api.io.hdfs

import java.net.URI
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.utils.URIBuilder
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.io.hdfs.{CsvGraphLoader, CsvGraphWriter}

import scala.language.implicitConversions

/**
  * Data source for loading graphs from HDFS.
  *
  * @param hadoopConfig Hadoop configuration
  * @param rootPath     root path containing one ore more graphs
  */
case class HdfsCsvPropertyGraphDataSource(
  hadoopConfig: Configuration,
  rootPath: String)(implicit val session: CAPSSession) extends CAPSPropertyGraphDataSource {

  private val fileSystem = FileSystem.get(hadoopConfig)

  override def graph(name: GraphName): PropertyGraph = CsvGraphLoader(graphPath(name), hadoopConfig).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    CsvGraphWriter(graph, graphPath(name), hadoopConfig).store()

  override def delete(name: GraphName): Unit =
    if (hasGraph(name)) fileSystem.delete(new Path(rootPath), /* recursive = */ true)

  override def graphNames: Set[GraphName] = fileSystem.listStatus(new Path(rootPath))
    .filter(f => f.isDirectory)
    .map(f => f.getPath.getName)
    .map(GraphName)
    .toSet

  override def hasGraph(name: GraphName): Boolean = fileSystem.exists(new Path(graphPath(name)))

  private[hdfs] def graphPath(name: GraphName): URI = {
    val uri = URI.create(rootPath)

    new URIBuilder()
      .setScheme("hdfs")
      .setPath(Paths.get(uri.getPath, name.value).toString)
      .build()
  }

}
