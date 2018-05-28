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
package org.opencypher.spark.api.io

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.spark.api.io.util.CachedDataSource._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSScanGraph
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.GraphConstructionFixture
import org.scalatest.BeforeAndAfterEach

class CachedDataSourceTest extends CAPSTestSuite with GraphConstructionFixture with BeforeAndAfterEach {

  private val testNamespace = caps.catalog.sessionNamespace
  private val testGraphName = GraphName("test")
  private val testDataSource = caps.catalog.source(testNamespace)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testDataSource.store(testGraphName, initGraph(s"CREATE (:A)"))
  }

  override protected def afterEach(): Unit = {
    if (testDataSource.hasGraph(testGraphName)) {
      testDataSource.graph(testGraphName).asCaps.unpersist()
      testDataSource.delete(testGraphName)
    }
    super.afterEach()
  }

  it("should cache the graph on first read") {
    val g0 = testDataSource.graph(testGraphName)
    assert(g0, StorageLevel.NONE)

    val cachedDataSource = testDataSource.withCaching
    val g1 = cachedDataSource.graph(testGraphName)
    assert(g1, StorageLevel.MEMORY_AND_DISK)

    assert(g0, StorageLevel.MEMORY_AND_DISK) // side effect for session ds

    cachedDataSource.hasGraph(testGraphName) should equal(true)
    testDataSource.hasGraph(testGraphName) should equal(true)
  }

  it("should cache the graph on first read with specific storage level") {
    val cachedDs = testDataSource.withCaching(StorageLevel.MEMORY_ONLY)
    val g = cachedDs.graph(testGraphName)
    assert(g, StorageLevel.MEMORY_ONLY)
  }

  it("should delete a graph and remove from cache") {
    val cachedDs = testDataSource.withCaching
    val g = cachedDs.graph(testGraphName)
    assert(g, StorageLevel.MEMORY_AND_DISK)

    cachedDs.delete(testGraphName)
    assert(g, StorageLevel.NONE)

    cachedDs.hasGraph(testGraphName) should equal(false)
    testDataSource.hasGraph(testGraphName) should equal(false)
  }

  private def assert(g: PropertyGraph, storageLevel: StorageLevel): Unit = {
    g.asInstanceOf[CAPSScanGraph].scans
      .map(_.table.df)
      .foreach(_.storageLevel should equal(storageLevel))
  }
}
