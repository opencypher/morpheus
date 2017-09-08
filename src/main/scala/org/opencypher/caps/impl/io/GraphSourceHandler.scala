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

import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

case class GraphSourceHandler(graphSourceFactories: Set[GraphSourceFactory],
                              mountPoints: Map[String, GraphSource],
                              graphSources: Set[GraphSource]) {

  def withGraphAt(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    if (Option(uri.getScheme).isDefined) loadFromURI(uri) else loadFromMountPoint(uri)

  private def loadFromURI(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    graphSources.find(_.sourceForGraphAt(uri)).getOrElse {
      graphSourceFactories.find(_.schemes.contains(uri.getScheme))
        .getOrElse(Raise.invalidArgument(graphSourceFactories.flatMap(_.schemes).mkString("[", ",", "]"), uri.getScheme))
        .sourceFor(uri)
    }.graph

  private def loadFromMountPoint(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    mountPoints
      .getOrElse(uri.getPath, Raise.invalidArgument(mountPoints.keySet.mkString("[", ",", "]"), uri.getPath))
      .graph

}
