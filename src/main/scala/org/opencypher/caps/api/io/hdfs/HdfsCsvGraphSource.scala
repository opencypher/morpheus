/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.io.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class HdfsCsvGraphSource(hadoopConfig: Configuration, path: String)
  extends GraphSource {

  override def handles(uri: URI): Boolean = {
    val hadoopURIString = Option(hadoopConfig.get("fs.defaultFS"))
      .getOrElse(Option(hadoopConfig.get("fs.default.name"))
      .getOrElse(Raise.invalidConnection("Neither fs.defaultFS nor fs.default.name found"))
    )

    val hadoopURI = URI.create(hadoopURIString)

    uri.getScheme == HdfsCsvGraphSource.protocol && hadoopURI.getHost == uri.getHost && hadoopURI.getPort == uri.getPort
  }

  override def get(implicit capsSession: CAPSSession): CAPSGraph =
    new CsvGraphLoader(path, hadoopConfig).load
}

object HdfsCsvGraphSource {
  val protocol = "hdfs+csv"
}

case class HdfsCsvGraphSourceFactory(hadoopConfiguration: Configuration)
  extends GraphSourceFactory {

  override val protocol: String = HdfsCsvGraphSource.protocol

  override def fromURI(uri: URI): GraphSource = {
    val host = uri.getHost
    val port = if (uri.getPort == -1) "" else s":${uri.getPort}"
    val defaultName = s"hdfs://$host$port"

    val hadoopConf = new Configuration(hadoopConfiguration)
    hadoopConf.set("fs.default.name", defaultName)
    HdfsCsvGraphSource(hadoopConf, uri.getPath)
  }
}
