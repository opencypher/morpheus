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
package org.opencypher.spark.api.io.file

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.TeamDataFixture

class FileCsvPropertyGraphDataSourceTest extends CAPSTestSuite with TeamDataFixture {

  private val testRootPath = getClass.getResource("/csv").getPath

  test("hasGraph should return true for existing graph") {
    val testGraphName = GraphName("sn")

    val dataSource = new FileCsvPropertyGraphDataSource(graphFolder = testRootPath)

    dataSource.hasGraph(testGraphName) should be(true)
  }

  test("hasGraph should return false for non-existing graph") {
    val testGraphName = GraphName("sn2")

    val dataSource = new FileCsvPropertyGraphDataSource(graphFolder = testRootPath)

    dataSource.hasGraph(testGraphName) should be(false)
  }

  test("graphNames should return all names of stored graphs") {
    val testGraphName1 = GraphName("sn")
    val testGraphName2 = GraphName("prod")
    val source = new FileCsvPropertyGraphDataSource(graphFolder = testRootPath)

    source.graphNames should equal(Set(testGraphName1, testGraphName2))
  }

  test("Load graph from file via DataSource") {
    val testGraphName = GraphName("sn")

    val dataSource = new FileCsvPropertyGraphDataSource(graphFolder = testRootPath)

    val graph = dataSource.graph(testGraphName)
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(csvTestGraphRels)
  }

  test("Load graph from file via Catalog") {
    val testNamespace = Namespace("myFS")
    val testGraphName = GraphName("sn")

    val dataSource = new FileCsvPropertyGraphDataSource(graphFolder = testRootPath)
    caps.registerSource(testNamespace, dataSource)

    val nodes = caps.cypher(s"USE GRAPH $testNamespace.$testGraphName MATCH (n) RETURN n")
    nodes.getRecords.asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)
    val edges = caps.cypher(s"USE GRAPH $testNamespace.$testGraphName MATCH ()-[r]->() RETURN r")
    edges.getRecords.asCaps.toDF().collect().toBag should equal(csvTestGraphRelsFromRecords)
  }
}
