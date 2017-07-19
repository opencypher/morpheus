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
package org.opencypher.spark_legacy.impl

import org.opencypher.spark_legacy.PropertyGraphFactory
import org.opencypher.spark.api.value._

final class CachingPropertyGraphFactory[T <: PropertyGraphFactory](val underlying: T) extends PropertyGraphFactory {

  private var graphCache: Option[underlying.Graph] = None

  override type Graph = underlying.Graph

  def addNode(labels: Set[String], properties: Properties) =
    flushAfter { underlying.addNode(labels, properties) }

  def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Properties) =
    flushAfter { underlying.addRelationship(startId, relationshipType, endId, properties) }

  override def graph: underlying.Graph = graphCache match {
    case Some(cachedGraph) =>
      cachedGraph

    case None =>
      val newGraph = underlying.graph
      graphCache = Some(newGraph)
      newGraph
  }

  override def clear(): Unit =
    flushAfter { underlying.clear() }

  // TODO: investigate IntelliJ warning
  private def flushAfter[V](f: => V) =
    try {
      f
    } finally  {
      graphCache = None
    }
}
