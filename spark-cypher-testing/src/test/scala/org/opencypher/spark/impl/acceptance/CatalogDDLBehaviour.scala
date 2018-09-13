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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.{GraphName, QualifiedGraphName}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintOptimizedRelationalPlan
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class CatalogDDLBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("CATALOG CREATE GRAPH") {
    it("supports CATALOG CREATE GRAPH on the session") {
      val inputGraph = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)

      caps.catalog.store("foo", inputGraph)

      val result = caps.cypher(
        """
          |CATALOG CREATE GRAPH bar {
          | FROM GRAPH foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val sessionSource = caps.catalog.source(caps.catalog.sessionNamespace)
      sessionSource.hasGraph(GraphName("bar")) shouldBe true
      sessionSource.graph(GraphName("bar")) shouldEqual inputGraph
      result.getGraph shouldBe None
      result.getRecords shouldBe None
    }
  }

  describe("CATALOG CREATE VIEW") {
    it("supports CATALOG CREATE VIEW") {
      val inputGraph = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)

      caps.catalog.store("foo", inputGraph)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar {
          | FROM GRAPH foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)
      caps.catalog.view(bar) shouldEqual inputGraph
    }

    it("supports CATALOG CREATE VIEW with parameters") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE (:B)
          |CREATE (:B)
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)
      caps.catalog.store("b", inputGraphB)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | FROM GRAPH $g2
          | MATCH (b: B)
          | CONSTRUCT
          |   CREATE (a)
          |   CREATE (b)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val resultGraph = caps.catalog.view(bar, List("a", "b"))
      resultGraph.nodes("n").size shouldBe 3
      resultGraph.nodes("a", CTNode("A")).size shouldBe 1
      resultGraph.nodes("b", CTNode("B")).size shouldBe 2
    }

    it("supports simple nested CATALOG CREATE VIEW with parameters") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A {val: 0})
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | CONSTRUCT
          |   CREATE (:A { val: a.val + 1 })
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val resultGraph = caps.catalog.view(bar, List("bar(bar(bar(a)))"))
      resultGraph.cypher("MATCH (n) RETURN n.val as val").records.toMaps should equal(Bag(CypherMap(
        "val" -> 4
      )))
    }

    it("supports simple nested CATALOG CREATE VIEW in a query") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A {val: 0})
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | CONSTRUCT
          |   CREATE (:A { val: a.val + 1 })
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val result = caps.cypher(
        """
          |FROM GRAPH bar(bar(bar(bar(a))))
          |MATCH (n)
          |RETURN n.val as val
        """.stripMargin)

      result.records.toMaps should equal(Bag(CypherMap(
        "val" -> 4
      )))
    }

    it("supports complex nested CATALOG CREATE VIEW with parameters") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE (:B)
          |CREATE (:B)
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)
      caps.catalog.store("b", inputGraphB)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | FROM GRAPH $g2
          | MATCH (b: B)
          | CONSTRUCT
          |   CREATE (a)
          |   CREATE (b)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val resultGraph = caps.catalog.view(bar, List("bar(a, b)", "b"))
//      resultGraph.nodes("n").capsRecords.show
//      resultGraph.nodes("n").size shouldBe 3
//      resultGraph.nodes("a", CTNode("A")).size shouldBe 1
//      resultGraph.nodes("b", CTNode("B")).size shouldBe 2
    }

    it("supports using CATALOG CREATE VIEW in a query") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE (:B)
          |CREATE (:B)
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)
      caps.catalog.store("b", inputGraphB)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | FROM GRAPH $g2
          | MATCH (b: B)
          | CONSTRUCT
          |   CREATE (a)
          |   CREATE (b)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val result = caps.cypher(
        """
          |FROM GRAPH bar(a, b)
          |MATCH (n)
          |RETURN n
        """.stripMargin)

      result.records.size shouldBe 3
    }

    it("supports using nested CATALOG CREATE VIEW in a query") {
      val inputGraphA = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)
      val inputGraphB = initGraph(
        """
          |CREATE (:B)
          |CREATE (:B)
        """.stripMargin)

      caps.catalog.store("a", inputGraphA)
      caps.catalog.store("b", inputGraphB)

      caps.cypher(
        """
          |CATALOG CREATE VIEW bar($g1, $g2) {
          | FROM GRAPH $g1
          | MATCH (a: A)
          | FROM GRAPH $g2
          | MATCH (b: B)
          | CONSTRUCT
          |   CREATE (a)
          |   CREATE (b)
          | RETURN GRAPH
          |}
        """.stripMargin)

      val bar = QualifiedGraphName("bar")
      caps.catalog.catalogNames should contain(bar)
      caps.catalog.viewNames should contain(bar)

      val result = caps.cypher(
        """
          |FROM GRAPH bar(bar(a, b), a)
          |MATCH (n)
          |RETURN n
        """.stripMargin)

      result.records.size shouldBe 3
    }

  }

  describe("DROP GRAPH") {
    it("can drop a session graph") {

      caps.catalog.store("foo", initGraph("CREATE (:A)"))

      val result = caps.cypher(
        """
          |CATALOG DROP GRAPH session.foo
        """.stripMargin
      )

      caps.catalog.source(caps.catalog.sessionNamespace).hasGraph(GraphName("foo")) shouldBe false
      result.getGraph shouldBe None
      result.getRecords shouldBe None
    }
  }
}
