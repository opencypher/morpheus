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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.GraphName
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.spark.io.CAPSPropertyGraphDataSource

/**
  * Data source for loading graphs from HDFS.
  *
  * @param hadoopConfig Hadoop configuration
  * @param rootPath     root path containing one ore more graphs
  */
class HdfsCsvPropertyGraphDataSource(
  hadoopConfig: Configuration,
  rootPath: String)(implicit val session: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph =
    CsvGraphLoader(s"$rootPath${File.separator}$name", hadoopConfig).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
