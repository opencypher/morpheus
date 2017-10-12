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
package org.opencypher.caps.impl.spark.io.file

import java.net.URI

import org.opencypher.caps.api.io.{CreateOrFail, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceImpl
import org.opencypher.caps.impl.spark.io.hdfs.CsvGraphLoader

case class FileCsvGraphSource(override val canonicalURI: URI)(implicit capsSession: CAPSSession)
  extends CAPSGraphSourceImpl {

  override def sourceForGraphAt(uri: URI): Boolean = {
    FileCsvGraphSourceFactory.supportedSchemes.contains(uri.getScheme)
  }

  override def graph: CAPSGraph =
    new CsvGraphLoader(canonicalURI.getPath, capsSession.sparkSession.sparkContext.hadoopConfiguration).load

  // TODO: Make better/cache?
  override def schema: Option[Schema] = None

  override def create: CAPSGraph =
    persist(CAPSGraph.empty, CreateOrFail)

  override def persist(graph: CAPSGraph, mode: PersistMode): CAPSGraph =
    ???

  override def delete(): Unit =
    ???
}
