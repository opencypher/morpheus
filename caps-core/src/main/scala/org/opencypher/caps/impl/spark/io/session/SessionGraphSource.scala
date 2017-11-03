/*
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
package org.opencypher.caps.impl.spark.io.session

import java.net.URI

import org.opencypher.caps.api.io.{CreateOrFail, Overwrite, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceImpl
import org.opencypher.caps.impl.spark.exception.Raise

case class SessionGraphSource(path: String)(implicit capsSession: CAPSSession)
  extends CAPSGraphSourceImpl {

  private var currentGraph: Option[CAPSGraph] = None

  override val canonicalURI: URI = URI.create(s"${SessionGraphSourceFactory.defaultScheme}:$path")

  override def sourceForGraphAt(uri: URI): Boolean =
    uri == canonicalURI

  override def create: CAPSGraph = store(CAPSGraph.empty, CreateOrFail)
  override def graph: CAPSGraph = currentGraph.getOrElse(Raise.graphNotFound(canonicalURI))
  override def schema: Option[Schema] = None

  override def store(graph: CAPSGraph, mode: PersistMode): CAPSGraph = mode match {
    case Overwrite =>
      currentGraph = Some(graph)
      graph

    case CreateOrFail if currentGraph.isEmpty =>
      currentGraph = Some(graph)
      graph

    case CreateOrFail =>
      Raise.graphAlreadyExists(canonicalURI)
  }

  override def delete(): Unit =
    currentGraph = None
}
