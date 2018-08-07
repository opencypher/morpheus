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
package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.{CypherResult, GraphName, Namespace}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.CypherGraphSources
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{CAPSNeo4jServerFixture, TeamDataFixture}

class Neo4jPropertyGraphDataSourceTest
  extends CAPSTestSuite
    with CAPSNeo4jServerFixture
    with TeamDataFixture {

  it("can read lists from Neo4j") {
    val dataSource = CypherGraphSources.neo4j(neo4jConfig)

    val graph = dataSource.graph(dataSource.entireGraphName).asCaps
    graph.cypher("MATCH (n) RETURN n.languages").records.iterator.toBag should equal(Bag(
      CypherMap("n.languages" -> Seq("German", "English", "Klingon")),
      CypherMap("n.languages" -> Seq()),
      CypherMap("n.languages" -> CypherNull),
      CypherMap("n.languages" -> CypherNull),
      CypherMap("n.languages" -> CypherNull)
    ))
  }

  it("should load a graph from Neo4j via DataSource") {
    val dataSource = CypherGraphSources.neo4j(neo4jConfig)

    val graph = dataSource.graph(dataSource.entireGraphName).asCaps
    graph.nodes("n").asCaps.toCypherMaps.collect.toBag should equal(teamDataGraphNodes)
    graph.relationships("r").asCaps.toCypherMaps.collect.toBag should equal(teamDataGraphRels)
  }

  it("should load a graph from Neo4j via catalog") {
    val testNamespace = Namespace("myNeo4j")
    val dataSource = CypherGraphSources.neo4j(neo4jConfig)
    val testGraphName = dataSource.entireGraphName

    caps.registerSource(testNamespace, dataSource)

    val nodes: CypherResult = caps.cypher(s"FROM GRAPH $testNamespace.$testGraphName MATCH (n) RETURN n")
    nodes.records.collect.toBag should equal(teamDataGraphNodes)

    val edges = caps.cypher(s"FROM GRAPH $testNamespace.$testGraphName MATCH ()-[r]->() RETURN r")
    edges.records.collect.toBag should equal(teamDataGraphRels)
  }

  it("should omit properties with unsupported types if corresponding flag is set") {
    neo4jConfig.cypher(s"""CREATE (n:Unsupported:${metaPrefix}test { foo: time(), bar: 42 })""")

    val dataSource = CypherGraphSources.neo4j(neo4jConfig, omitIncompatibleProperties = true)
    val graph = dataSource.graph(GraphName("test")).asCaps
    val nodes = graph.nodes("n").asCaps.toCypherMaps.collect.toList
    nodes.size shouldBe 1
    nodes.head.value match {
      case n: CAPSNode => n should equal(CAPSNode(n.id, Set("Unsupported"), CypherMap("bar" -> 42L)))
      case other => IllegalArgumentException("a CAPSNode", other)
    }
  }

  it("should throw exception if properties with unsupported types are being imported") {
    an[UnsupportedOperationException] should be thrownBy {
      neo4jConfig.cypher(s"""CREATE (n:Unsupported:${metaPrefix}test { foo: time(), bar: 42 })""")

      val dataSource = CypherGraphSources.neo4j(neo4jConfig)
      val graph = dataSource.graph(GraphName("test")).asCaps
      graph.nodes("n").asCaps.toCypherMaps.collect.toList
    }
  }
}
