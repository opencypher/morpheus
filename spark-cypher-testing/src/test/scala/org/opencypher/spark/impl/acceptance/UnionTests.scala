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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.value.{MorpheusNode, MorpheusRelationship}
import org.opencypher.spark.testing.MorpheusTestSuite

class UnionTests extends MorpheusTestSuite with ScanGraphInit {

  describe("tabular union all") {
    it("unions simple queries") {
      val result = morpheus.cypher(
        """
          |RETURN 1 AS one
          |UNION ALL
          |RETURN 2 AS one
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2)
      ))
    }

    it("supports stacked union all") {
      val result = morpheus.cypher(
        """
          |RETURN 1 AS one
          |UNION ALL
          |RETURN 2 AS one
          |UNION ALL
          |RETURN 2 AS one
          |UNION ALL
          |RETURN 3 AS one
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2),
        CypherMap("one" -> 2),
        CypherMap("one" -> 3)
      ))
    }

    it("supports union all with UNWIND") {
      val result = morpheus.cypher(
        """
          |UNWIND [1, 2] AS i
          |RETURN i
          |UNION ALL
          |UNWIND [1, 2, 6] AS i
          |RETURN i
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("i" -> 1),
        CypherMap("i" -> 2),
        CypherMap("i" -> 1),
        CypherMap("i" -> 2),
        CypherMap("i" -> 6)
      ))
    }

    it("supports union all with MATCH on nodes") {
      val g = initGraph(
        """
          |CREATE (a: A {val: "foo"})
          |CREATE (b: B {bar: "baz"})
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A)
          |RETURN a AS node
          |UNION ALL
          |MATCH (b:B)
          |RETURN b AS node
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("node" -> MorpheusNode(0, Set("A"), CypherMap("val" -> "foo"))),
        CypherMap("node" -> MorpheusNode(1, Set("B"), CypherMap("bar" -> "baz")))
      ))
    }

    it("supports union all with MATCH on nodes and relationships") {
      val g = initGraph(
        """
          |CREATE (a: A {val: "foo"})
          |CREATE (b: B {bar: "baz"})
          |CREATE (a)-[:REL1 {foo: 42}]->(b)
          |CREATE (b)-[:REL2 {bar: true}]->(a)
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A)-[r]->()
          |RETURN a AS node, r AS rel
          |UNION ALL
          |MATCH (b:B)-[r]->()
          |RETURN b AS node, r AS rel
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("node" -> MorpheusNode(0, Set("A"), CypherMap("val" -> "foo")), "rel" -> MorpheusRelationship(2, 0, 1, "REL1", CypherMap("foo" -> 42))),
        CypherMap("node" -> MorpheusNode(1, Set("B"), CypherMap("bar" -> "baz")), "rel" -> MorpheusRelationship(3, 1, 0, "REL2", CypherMap("bar" -> true)))
      ))
    }
  }

  describe("tabular union") {
    it("unions simple queries") {
      val result = morpheus.cypher(
        """
          |RETURN 1 AS one
          |UNION
          |RETURN 2 AS one
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2)
      ))
    }

    it("unions simple queries with duplicates") {
      val result = morpheus.cypher(
        """
          |RETURN 1 AS one
          |UNION
          |RETURN 1 AS one
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("one" -> 1)
      ))
    }

    it("supports stacked union") {
      val result = morpheus.cypher(
        """
          |RETURN 1 AS one
          |UNION
          |RETURN 2 AS one
          |UNION
          |RETURN 2 AS one
          |UNION
          |RETURN 3 AS one
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2),
        CypherMap("one" -> 3)
      ))
    }

    it("supports union with UNWIND") {
      val result = morpheus.cypher(
        """
          |UNWIND [1, 2] AS i
          |RETURN i
          |UNION
          |UNWIND [1, 2, 6] AS i
          |RETURN i
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("i" -> 1),
        CypherMap("i" -> 2),
        CypherMap("i" -> 6)
      ))
    }

    it("supports union with MATCH on nodes") {
      val g = initGraph(
        """
          |CREATE (a: A {val: "foo"})
          |CREATE (b: B {bar: "baz"})
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A), (b:B)
          |RETURN a AS node1, b AS node2
          |UNION
          |MATCH (b:B), (a:A)
          |RETURN b AS node1, a AS node2
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("node1" -> MorpheusNode(0, Set("A"), CypherMap("val" -> "foo")), "node2" -> MorpheusNode(1, Set("B"), CypherMap("bar" -> "baz"))),
        CypherMap("node1" -> MorpheusNode(1, Set("B"), CypherMap("bar" -> "baz")), "node2" -> MorpheusNode(0, Set("A"), CypherMap("val" -> "foo")))
      ))
    }

    it("supports union on duplicate nodes") {
      val g = initGraph(
        """
          |CREATE (a: A {val: "foo"})
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A)
          |RETURN a AS node
          |UNION
          |MATCH (a:A)
          |RETURN a AS node
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("node" -> MorpheusNode(0, Set("A"), CypherMap("val" -> "foo")))
      ))
    }

    it("supports union on duplicate relationships") {
      val g = initGraph(
        """
          |CREATE (a)
          |CREATE (a)-[:REL {val: 42}]->(a)
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH ()-[r]->()
          |RETURN r AS rel
          |UNION
          |MATCH ()-[r]->()
          |RETURN r AS rel
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("rel" -> MorpheusRelationship(1, 0, 0, "REL", CypherMap("val" -> 42)))
      ))
    }
  }

  describe("Graph union all") {
    it("union all on graphs") {
      val a = initGraph("CREATE ()")
      morpheus.catalog.source(morpheus.catalog.sessionNamespace).store(GraphName("a"), a)
      morpheus.catalog.source(morpheus.catalog.sessionNamespace).store(GraphName("b"), a)
      val result = morpheus.cypher(
        """
          |FROM a
          |RETURN GRAPH
          |UNION ALL
          |FROM b
          |RETURN GRAPH
        """.stripMargin)

      result.graph.nodes("n").size should equal(2)
    }

    it("union all fails on graphs with common properties") {
      val a = initGraph("CREATE (:one{test:1})")
      val b = initGraph("CREATE (:one{test:'hello'})")
      morpheus.catalog.source(morpheus.catalog.sessionNamespace).store(GraphName("a"), a)
      morpheus.catalog.source(morpheus.catalog.sessionNamespace).store(GraphName("b"), b)
      val e: SchemaException = the[SchemaException] thrownBy {
        morpheus.cypher(
          """
            |FROM a
            |RETURN GRAPH
            |UNION ALL
            |FROM b
            |RETURN GRAPH
          """.stripMargin).graph
      }

      e.getMessage should (include("one") and include("test") and include("STRING") and include("INTEGER"))
    }
  }
}
