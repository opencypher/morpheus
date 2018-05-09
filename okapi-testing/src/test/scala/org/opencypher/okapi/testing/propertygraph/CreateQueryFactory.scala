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
package org.opencypher.okapi.testing.propertygraph

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag._
import org.opencypher.okapi.testing.{Bag, BaseTestSuite}

class CreateQueryFactory extends BaseTestSuite {

  test("parse single node create statement") {
    val graph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"})
      """.stripMargin)

    graph.nodes should equal(Seq(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in single create statement") {
    val graph: InMemoryTestGraph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice")),
      InMemoryTestNode(1, Set("Person"), CypherMap("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in separate create statements") {
    val graph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice")),
      InMemoryTestNode(1, Set("Person"), CypherMap("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes connected by relationship") {
    val graph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 42}]->(b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice")),
      InMemoryTestNode(1, Set("Person"), CypherMap("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      InMemoryTestRelationship(2, 0, 1, "KNOWS", CypherMap("since" -> 42))
    ))
  }

  test("parse multiple nodes and relationship in separate create statements") {
    val graph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (a)-[:KNOWS {since: 42}]->(b)
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice")),
      InMemoryTestNode(1, Set("Person"), CypherMap("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      InMemoryTestRelationship(2, 0, 1, "KNOWS", CypherMap("since" -> 42))
    ))
  }

  test("simple unwind") {
    val graph = CreateGraphFactory(
      """
        |UNWIND [1,2,3] as i
        |CREATE (a {val: i})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set(), CypherMap("val" -> 1)),
      InMemoryTestNode(1, Set(), CypherMap("val" -> 2)),
      InMemoryTestNode(2, Set(), CypherMap("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("stacked unwind") {
    val graph = CreateGraphFactory(
      """
        |UNWIND [1,2,3] AS i
        |UNWIND [4] AS j
        |CREATE (a {val1: i, val2: j})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set(), CypherMap("val1" -> 1, "val2" -> 4)),
      InMemoryTestNode(1, Set(), CypherMap("val1" -> 2, "val2" -> 4)),
      InMemoryTestNode(2, Set(), CypherMap("val1" -> 3, "val2" -> 4))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("unwind with variable reference") {
    val graph = CreateGraphFactory(
      """
        |UNWIND [[1,2,3]] AS i
        |UNWIND i AS j
        |CREATE (a {val: j})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set(), CypherMap("val" -> 1)),
      InMemoryTestNode(1, Set(), CypherMap("val" -> 2)),
      InMemoryTestNode(2, Set(), CypherMap("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("unwind with parameter reference") {
    val graph = CreateGraphFactory(
      """
        |UNWIND $i AS j
        |CREATE (a {val: j})
      """.stripMargin, Map("i" -> List(1, 2, 3)))

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set(), CypherMap("val" -> 1)),
      InMemoryTestNode(1, Set(), CypherMap("val" -> 2)),
      InMemoryTestNode(2, Set(), CypherMap("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("create statement with property reference") {
    val graph = CreateGraphFactory(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: a.name})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      InMemoryTestNode(0, Set("Person"), CypherMap("name" -> "Alice")),
      InMemoryTestNode(1, Set("Person"), CypherMap("name" -> "Alice"))
    ))

    graph.relationships should be(Seq.empty)
  }
}
