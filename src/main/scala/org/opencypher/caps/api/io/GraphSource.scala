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
package org.opencypher.caps.api.io

import java.net.URI

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

trait GraphSource {

  def handles(uri: URI): Boolean

  def get(implicit capsSession: CAPSSession): CAPSGraph
}

trait GraphSourceFactory {

  def protocol: String

  def fromURI(uri: URI): GraphSource
}

case class GraphSourceHandler(graphSourceFactories: Set[GraphSourceFactory],
                              mountPoints: Map[String, GraphSource],
                              graphSources: Set[GraphSource]) {

  def withGraphAt(uri: URI, alias: String)(implicit capsSession: CAPSSession): CAPSGraph =
    if (uri.getScheme != null) loadFromURI(uri) else loadFromMountPoint(uri)

  private def loadFromURI(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    graphSources.find(_.handles(uri)).getOrElse {
      graphSourceFactories.find(_.protocol == uri.getScheme)
        .getOrElse(Raise.invalidArgument(graphSourceFactories.map(_.protocol).mkString("[", ",", "]"), uri.getScheme))
        .fromURI(uri)
    }.get

  private def loadFromMountPoint(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    mountPoints
      .getOrElse(uri.getPath, Raise.invalidArgument(mountPoints.keySet.mkString("[", ",", "]"), uri.getPath))
      .get
}