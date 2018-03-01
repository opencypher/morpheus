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

import org.opencypher.okapi.api.graph.{CypherResult, GraphName, Namespace}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{Neo4jServerFixture, TeamDataFixture}

import scala.collection.Bag

class Neo4jPropertyGraphDataSourceTest
  extends CAPSTestSuite
    with Neo4jServerFixture
    with TeamDataFixture {

  it("can read lists from Neo4j") {
    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig)

    val graph = dataSource.graph(neo4jDefaultGraphName).asCaps
    graph.cypher("MATCH (n) RETURN n.languages").records.iterator.toBag should equal(Bag(
      CypherMap("n.languages" -> Seq("German", "English", "Klingon")),
      CypherMap("n.languages" -> Seq()),
      CypherMap("n.languages" -> CypherNull),
      CypherMap("n.languages" -> CypherNull),
      CypherMap("n.languages" -> CypherNull)
    ))
  }

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
    val source = new Neo4jPropertyGraphDataSource(neo4jConfig,
      Map(testGraphName1 -> defaultQuery, testGraphName2 -> defaultQuery))
    source.graphNames should equal(Set(testGraphName1, testGraphName2))
  }

  test("Load graph from Neo4j via DataSource") {
    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig)

    val graph = dataSource.graph(neo4jDefaultGraphName).asCaps
    graph.nodes("n").toCypherMaps.collect().toBag should equal(teamDataGraphNodes)
    graph.relationships("rel").toDF().collect().toBag should equal(teamDataGraphRels)
  }

  test("Load graph from Neo4j via Catalog") {
    val testNamespace = Namespace("myNeo4j")
    val testGraphName = neo4jDefaultGraphName

    val dataSource = new Neo4jPropertyGraphDataSource(neo4jConfig)

    caps.registerSource(testNamespace, dataSource)

    val nodes: CypherResult = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH (n) RETURN n")
    nodes.getRecords.collect.toBag should equal(teamDataGraphNodes)

    val edges = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH ()-[r]->() RETURN r")
    edges.getRecords.collect.toBag should equal(teamDataGraphRels)
  }
}
