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
package org.opencypher.caps.impl.spark.io.session

import java.net.URI

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.impl.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{CreateOrFail, Overwrite, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.CAPSGraph
import org.opencypher.caps.impl.spark.io.CAPSPropertyGraphDataSource

case class SessionPropertyGraphDataSource(path: String)(implicit val session: CAPSSession)
    extends CAPSPropertyGraphDataSource {

  private var currentGraph: Option[CAPSGraph] = None

  override val canonicalURI: URI = URI.create(s"${SessionPropertyGraphDataSourceFactory.defaultScheme}:$path")

  override def sourceForGraphAt(uri: URI): Boolean =
    uri == canonicalURI

  override def create: CAPSGraph = store(CAPSGraph.empty, CreateOrFail)

  override def graph: CAPSGraph = currentGraph.getOrElse(throw IllegalArgumentException(s"a graph at $canonicalURI"))

  override def schema: Option[Schema] = None

  override def store(graph: PropertyGraph, mode: PersistMode): CAPSGraph = {
    val capsGraph = graph.asCaps
    mode match {
      case Overwrite =>
        currentGraph = Some(capsGraph)
        capsGraph

      case CreateOrFail if currentGraph.isEmpty =>
        currentGraph = Some(capsGraph)
        capsGraph

      case CreateOrFail =>
        throw UnsupportedOperationException(s"Overwriting the session graph")
    }
  }

  override def delete(): Unit =
    currentGraph = None
}
