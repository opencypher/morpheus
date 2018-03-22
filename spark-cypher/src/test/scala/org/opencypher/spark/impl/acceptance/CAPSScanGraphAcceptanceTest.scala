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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan}
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

import org.opencypher.spark.impl.CAPSConverters._

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")
  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")


  it("supports UNION ALL") {
    testGraph1.unionAll(testGraph2).cypher("""MATCH (n) RETURN DISTINCT id(n)""").getRecords.size should equal(2)
  }

  it("supports MERGE in CONSTRUCT") {
    PrintLogicalPlan.set()
    PrintFlatPlan.set()
    PrintPhysicalPlan.set()

    val res = testGraph1.unionAll(testGraph2).cypher(
      """
        |MATCH (n),(m),(c)
        |WHERE n.name = 'Mats' AND m.name = 'Phil'
        |CONSTRUCT {
        | MERGE (n)
        | MERGE (m)
        | CREATE (n)-[r:KNOWS]->(m)
        |}
        |RETURN GRAPH
      """.stripMargin)


    res.getGraph.relationships("r").asCaps.data.show()

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 1
  }

  it("merges multiple relationships") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |WITH DISTINCT n, m
        |CONSTRUCT {
        | MERGE (n)
        | MERGE (m)
        | CREATE (n)-[r:KNOWS]->(m)
        |}
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 2
  }

  it("merges multiple relationships 2") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |CONSTRUCT {
        | MERGE (n)
        | MERGE (m)
        | CREATE (n)-[r:KNOWS]->(m)
        |}
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 2
  }



}
