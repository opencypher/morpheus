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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{CreateOrFail, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.spark.io.CAPSPropertyGraphDataSource

case class HdfsCsvPropertyGraphDataSource(override val canonicalURI: URI, hadoopConfig: Configuration, path: String)(
    implicit val session: CAPSSession)
    extends CAPSPropertyGraphDataSource {

  import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvPropertyGraphDataSourceFactory.supportedSchemes

  override def sourceForGraphAt(uri: URI): Boolean = {
    val hadoopURIString = Option(hadoopConfig.get("fs.defaultFS"))
      .getOrElse(
        Option(hadoopConfig.get("fs.default.name"))
          .getOrElse(throw IllegalArgumentException("a value for fs.defaultFS or fs.default.name")))
    val hadoopURI = URI.create(hadoopURIString)
    supportedSchemes.contains(uri.getScheme) && hadoopURI.getHost == uri.getHost && hadoopURI.getPort == uri.getPort
  }

  override def graph: CAPSGraph = CsvGraphLoader(path, hadoopConfig).load

  // TODO: Make better/cache?
  override def schema: Option[Schema] = None

  override def create: CAPSGraph =
    store(CAPSGraph.empty, CreateOrFail)

  override def store(graph: PropertyGraph, mode: PersistMode): CAPSGraph =
    ???

  override def delete(): Unit =
    ???
}
