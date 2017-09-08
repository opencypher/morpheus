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
package org.opencypher.caps.impl.io

import java.net.URI

import org.opencypher.caps.api.io.session.SessionGraphSourceFactory
import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class GraphSourceHandler(sessionGraphSourceFactory: SessionGraphSourceFactory,
                              additionalGraphSourceFactories: Set[GraphSourceFactory]) {

  private val factoriesByScheme: Map[String, GraphSourceFactory] = {
    val allFactories = additionalGraphSourceFactories + sessionGraphSourceFactory
    val entries = allFactories.flatMap(factory => factory.schemes.map(scheme => scheme -> factory))
    if (entries.size == entries.map(_._1).size)
      entries.toMap
    else
      Raise.invalidArgument(
        "At most one graph source factory per URI scheme",
        s"Factories for schemes: ${allFactories.map(factory => factory.name -> factory.schemes.mkString("[", ", ", "]")).mkString(",")}")
  }

  def mountSourceAt(source: GraphSource, uri: URI)(implicit capsSession: CAPSSession): Unit =
    sessionGraphSourceFactory.mountSourceAt(source, uri)

  def graphAt(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    factoriesByScheme
      .get(uri.getScheme)
      .map(_.sourceFor(uri).graph)
      .getOrElse(Raise.graphNotFound(uri))
}
