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

class FileCsvPropertyGraphDataSource(rootPath: String)(implicit val session: CAPSSession)
  extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = CsvGraphLoader(graphPath(name)).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw new UnsupportedOperationException("'store' operation is not supported by the FileCSV data source")

  override def delete(name: GraphName): Unit =
    if (hasGraph(name)) Files.delete(Paths.get(graphPath(name)))

  override def graphNames: Set[GraphName] = Files.list(Paths.get(rootPath)).iterator().asScala
    .filter(p => Files.isDirectory(p))
    .map(p => p.getFileName.toString)
    .map(GraphName)
    .toSet

  override def hasGraph(name: GraphName): Boolean = Files.exists(Paths.get(graphPath(name)))

  private def graphPath(name: GraphName): String = s"$rootPath${File.separator}$name"
}
