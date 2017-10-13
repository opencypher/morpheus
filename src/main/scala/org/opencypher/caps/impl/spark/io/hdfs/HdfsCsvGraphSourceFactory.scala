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
package org.opencypher.caps.impl.spark.io.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.opencypher.caps.api.spark.io._
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceFactoryImpl

case object HdfsCsvGraphSourceFactory extends CAPSGraphSourceFactoryCompanion("hdfs+csv")

case class HdfsCsvGraphSourceFactory(hadoopConfiguration: Configuration)
  extends CAPSGraphSourceFactoryImpl[HdfsCsvGraphSource](HdfsCsvGraphSourceFactory) {

  override protected def sourceForURIWithSupportedScheme(uri: URI)(implicit capsSession: CAPSSession): HdfsCsvGraphSource = {
    val host = uri.getHost
    val port = if (uri.getPort == -1) "" else s":${uri.getPort}"
    val canonicalURIString = s"hdfs://$host$port${uri.getPath}"
    val canonicalURI = URI.create(canonicalURIString)

    val hadoopConf = new Configuration(hadoopConfiguration)
    hadoopConf.set("fs.default.name", canonicalURIString)
    HdfsCsvGraphSource(canonicalURI, hadoopConf, uri.getPath)
  }
}
