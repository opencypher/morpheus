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
package org.opencypher.spark.impl.io.neo4j

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.spark.test.fixture.{CAPSSessionFixture, Neo4jServerFixture, SparkSessionFixture, TeamDataFixture}
import org.scalatest.mockito.MockitoSugar

class Neo4jPropertyGraphDataSourceTest
  extends BaseTestSuite
    with SparkSessionFixture
    with CAPSSessionFixture
    with Neo4jServerFixture
    with TeamDataFixture
    with MockitoSugar {

  test("hasGraph should return true for existing graph") {
    val testGraphName = GraphName("sn")

    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      testGraphName -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")
    ))

    dataSource.hasGraph(testGraphName) should be(true)
  }

  test("hasGraph should return false for non-existing graph") {
    val testGraphName = GraphName("sn")

    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      GraphName("sn2") -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")
    ))

    dataSource.hasGraph(testGraphName) should be(false)
  }

  test("graphNames should return all names of stored graphs") {
    val testGraphName1 = GraphName("test1")
    val testGraphName2 = GraphName("test2")
    val source = new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      testGraphName1 -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r"),
      testGraphName2 -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")
    ))
    source.graphNames should equal(Set(testGraphName1, testGraphName2))
  }

  test("Load graph from Neo4j via DataSource") {
    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      GraphName("foo") -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")
    ))

    val graph = dataSource.graph(GraphName("foo")).asCaps
    graph.nodes("n").toDF().collect().toBag should equal(teamDataGraphNodes)
    graph.relationships("rel").toDF().collect().toBag should equal(teamDataGraphRels)
  }

  test("Load graph from Neo4j via Catalog") {
    val testNamespace = Namespace("myNeo4j")
    val testGraphName = GraphName("foo")

    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      testGraphName -> ("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")
    ))

    caps.registerSource(testNamespace, dataSource)

    val nodes = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH (n) RETURN n")
    nodes.records.asCaps.toDF().collect().toBag should equal(teamDataGraphNodes)

    val edges = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH ()-[r]->() RETURN r")
    edges.records.asCaps.toDF().collect().toBag should equal(teamDataGraphRels)
  }
}
