/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintOptimizedRelationalPlan, PrintRelationalPlan}
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.value.CAPSEntity._
import org.opencypher.spark.testing.fixture.{GraphConstructionFixture, RecordsVerificationFixture, TeamDataFixture}

class UnionGraphTest extends CAPSGraphTest
  with GraphConstructionFixture
  with RecordsVerificationFixture
  with TeamDataFixture {

  import CAPSGraphTestData._

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")
  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  it("supports UNION ALL") {
    testGraph1.unionAll(testGraph2).cypher("""MATCH (n) RETURN DISTINCT id(n)""").records.size should equal(2)
  }

  it("supports UNION ALL on identical graphs") {
    val g = initGraph("CREATE ()")
    val union = g.unionAll(g)
    union.nodes("n").size shouldBe 2
  }

  it("supports union in CONSTRUCT:ed graphs") {
    val g1 = initGraph("CREATE ()-[:FOO]->()")
    val g2 = initGraph("CREATE ()")
    caps.catalog.store("g1", g1)
    caps.catalog.store("g2", g2)
    val union = caps.cypher("CONSTRUCT ON g1, g2 RETURN GRAPH").graph

    union.nodes("n").size shouldBe 3
  }

  test("Node scan from multiple single node CAPSRecords") {
    val unionGraph = initGraph(`:Person`).unionAll(initGraph(`:Book`))
    val nodes = unionGraph.nodes("n")
    val cols = Seq(
      n,
      nHasLabelBook,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName,
      nHasPropertyTitle,
      nHasPropertyYear
    )
    val data = Bag(
      Row(0L.withPrefix(0).toList, false, true, 23L, "Mats", null, null),
      Row(1L.withPrefix(0).toList, false, true, 42L, "Martin", null, null),
      Row(2L.withPrefix(0).toList, false, true, 1337L, "Max", null, null),
      Row(3L.withPrefix(0).toList, false, true, 9L, "Stefan", null, null),
      Row(0L.withPrefix(1).toList, true, false, null, null, "1984", 1949L),
      Row(1L.withPrefix(1).toList, true, false, null, null, "Cryptonomicon", 1999L),
      Row(2L.withPrefix(1).toList, true, false, null, null, "The Eye of the World", 1990L),
      Row(3L.withPrefix(1).toList, true, false, null, null, "The Circle", 2013L)
    )

    verify(nodes, cols, data)
  }

  test("Returns only distinct results") {
    val scanGraph1 = caps.graphs.create(personTable)
    val scanGraph2 = caps.graphs.create(personTable)

    val unionGraph = scanGraph1.unionAll(scanGraph2)
    val nodes = unionGraph.nodes("n")

    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row(1L.withPrefix(0).toList, true, 23L, "Mats"),
      Row(2L.withPrefix(0).toList, true, 42L, "Martin"),
      Row(3L.withPrefix(0).toList, true, 1337L, "Max"),
      Row(4L.withPrefix(0).toList, true, 9L, "Stefan"),
      Row(1L.withPrefix(1).toList, true, 23L, "Mats"),
      Row(2L.withPrefix(1).toList, true, 42L, "Martin"),
      Row(3L.withPrefix(1).toList, true, 1337L, "Max"),
      Row(4L.withPrefix(1).toList, true, 9L, "Stefan")
    )

    verify(nodes, cols, data)
  }

  it("union all on graphs") {
    PrintIr.set()
    PrintLogicalPlan.set()
    PrintRelationalPlan.set()
    val a = initGraph("CREATE ()")
    caps.catalog.source(caps.catalog.sessionNamespace).store(GraphName("a"), a)
    caps.catalog.source(caps.catalog.sessionNamespace).store(GraphName("b"), a)
    //todo: maybeRelational.graph  how to return graph?
    val result = caps.cypher(
      """
        |FROM a
        |RETURN GRAPH
        |UNION ALL
        |FROM b
        |RETURN GRAPH
      """.stripMargin)

    //node size = 2?
    result.maybeRelational.get.graph.nodes("n").size should equal(2)
  }
}
