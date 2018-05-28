/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.api.io.util

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.spark.impl.CAPSConverters._

import scala.collection.mutable

/**
  * Wraps a [[PropertyGraphDataSource]] and introduces a caching mechanism. First time a graph is read, it is cached in
  * Spark according to the given [[StorageLevel]] and put in the datasource internal cache. When a graph is removed from
  * the data source, it is uncached in Spark and removed from the datasource internal cache.
  *
  * @param dataSource property graph data source
  * @param storageLevel storage level for caching the graph
  */
case class CachedDataSource(
  dataSource: PropertyGraphDataSource,
  storageLevel: StorageLevel) extends PropertyGraphDataSource {

  private val cache: mutable.Map[GraphName, PropertyGraph] = mutable.Map.empty

  override def graph(name: GraphName): PropertyGraph = cache.getOrElse(name, {
    val g = dataSource.graph(name)
    g.asCaps.persist(storageLevel)
    cache.put(name, g)
    g
  })

  override def delete(name: GraphName): Unit = cache.get(name) match {
    case Some(g) =>
      g.asCaps.unpersist()
      cache.remove(name)
      dataSource.delete(name)

    case None =>
      dataSource.delete(name)
  }

  override def hasGraph(name: GraphName): Boolean = cache.contains(name) || dataSource.hasGraph(name)

  override def schema(name: GraphName): Option[Schema] = dataSource.schema(name)

  override def store(name: GraphName, graph: PropertyGraph): Unit = dataSource.store(name, graph)

  override def graphNames: Set[GraphName] = dataSource.graphNames
}

object CachedDataSource {

  implicit class WithCacheSupport(val ds: PropertyGraphDataSource) {

    def withCaching: CachedDataSource =
      CachedDataSource(ds, StorageLevel.MEMORY_AND_DISK)

    def withCaching(storageLevel: StorageLevel): CachedDataSource =
      CachedDataSource(ds, storageLevel)
  }

}
