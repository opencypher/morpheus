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
package org.opencypher.caps.cosc.datasource

import java.net.URI

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{CreateOrFail, Overwrite, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.cosc.{COSCGraph, COSCSession}
import org.opencypher.caps.cosc.COSCConverters._

case class COSCSessionPropertyGraphDataSource(path: String)(implicit val session: COSCSession)
  extends COSCPropertyGraphDataSource {

  private var currentGraph: Option[COSCGraph] = None

  override val canonicalURI: URI = URI.create(s"${COSCSessionPropertyGraphDataSourceFactory.defaultScheme}:$path")

  override def sourceForGraphAt(uri: URI): Boolean =
    uri == canonicalURI

  override def create: COSCGraph = store(COSCGraph.empty, CreateOrFail)

  override def graph: COSCGraph = currentGraph.getOrElse(throw IllegalArgumentException(s"a graph at $canonicalURI"))

  override def schema: Option[Schema] = None

  override def store(graph: PropertyGraph, mode: PersistMode): COSCGraph = {
    val coscGraph = graph.asCosc
    mode match {
      case Overwrite =>
        currentGraph = Some(coscGraph)
        coscGraph

      case CreateOrFail if currentGraph.isEmpty =>
        currentGraph = Some(coscGraph)
        coscGraph

      case CreateOrFail =>
        throw UnsupportedOperationException(s"Overwriting the session graph")
    }
  }

  override def delete(): Unit =
    currentGraph = None
}
