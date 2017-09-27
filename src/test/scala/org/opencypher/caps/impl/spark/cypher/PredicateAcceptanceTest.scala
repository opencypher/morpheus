/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.cypher

import org.opencypher.caps.api.value.{CypherList, CypherMap}
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.immutable.Bag

class PredicateAcceptanceTest extends CAPSTestSuite {

  test("exists()") {
    val given = TestGraph("({id: 1l}), ({id: 2l}), ({other: 'foo'}), ()")

    val result = given.cypher("MATCH (n) WHERE exists(n.id) RETURN n.id")

    result.records.toMaps should equal(Bag(
      CypherMap("n.id" -> 1),
      CypherMap("n.id" -> 2)
    ))
  }

  test("in") {
    // Given
    val given = TestGraph("""(:A {val: 1L}), (:A {val: 2L}), (:A {val: 3L})""")

    // When
    val result = given.cypher("MATCH (a:A) WHERE a.val IN [-1, 2, 5, 0] RETURN a.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.val" -> 2)
    ))

    // And
    result.graphs shouldBe empty
  }

  // TODO: Fix this
  ignore("in with parameter") {
    // Given
    val given = TestGraph("""(:A {val: 1L}), (:A {val: 2L}), (:A {val: 3L})""")

    // When
    val result = given.cypher("MATCH (a:A) WHERE a.val IN $list RETURN a.val", Map("list" -> CypherList(Seq(-1, 2, 5, 0))))

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.val" -> 2)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("or") {
    // Given
    val given = TestGraph("""(:A {val: 1L}), (:A {val: 2L}), (:A {val: 3L})""")

    // When
    val result = given.cypher("MATCH (a:A) WHERE a.val = 1 OR a.val = 2 RETURN a.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.val" -> 1),
      CypherMap("a.val" -> 2)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("or on labels") {
    // Given
    val given = TestGraph("""(:A {val: 1L}), (:B {val: 2L}), (:C {val: 3L})""")

    // When
    val result = given.cypher("MATCH (a) WHERE a:A OR a:B RETURN a.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.val" -> 1),
      CypherMap("a.val" -> 2)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("or with and") {
    // Given
    val given = TestGraph("""(:A {val: 1L, name: 'a'}),
                            |(:A {val: 2L, name: 'a'}),
                            |(:A {val: 3L, name: 'e'}),
                            |(:A {val: 4L}),
                            |(:A {val: 5L, name: 'e'})
                          """.stripMargin)

    // When
    val result = given.cypher("MATCH (a:A) WHERE a.val = 1 OR (a.val >= 4 AND a.name = 'e') RETURN a.val, a.name")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.val" -> 1, "a.name" -> "a"),
      CypherMap("a.val" -> 5, "a.name" -> "e")
    ))

    // And
    result.graphs shouldBe empty
  }

  test("equality between properties") {
    // Given
    val given = TestGraph("""(:A {val: 1L})-->(:B {p: 2L}),
                            |(:A {val: 2L})-->(:B {p: 1L}),
                            |(:A {val: 100L})-->(:B {p: 100L}),
                            |(:A {val: 1L})-->(:B),
                            |(:A)-->(:B {p: 2L}),
                            |(:A)-->(:B),
                          """.stripMargin)

    // When
    val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val = b.p RETURN b.p")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("b.p" -> 100)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("less than") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val < m.val RETURN n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val" -> 4)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("less than or equal") {
    // Given
    val given = TestGraph("""(:Node {id: 1L, val: 4L})-->(:Node {id: 2L, val: 5L})-->(:Node {id: 3L, val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val <= m.val RETURN n.id, n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.id" -> 1, "n.val" -> 4),
      CypherMap("n.id" -> 2, "n.val" -> 5)
    ))
    // And
    result.graphs shouldBe empty
  }

  test("greater than") {
    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val:5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val > m.val RETURN n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val" -> 5)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("greater than or equal") {
    // Given
    val given = TestGraph("""(:Node {id: 1L, val: 4L})-->(:Node {id: 2L, val: 5L})-->(:Node {id: 3L, val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val >= m.val RETURN n.id, n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.id" -> 2, "n.val" -> 5),
      CypherMap("n.id" -> 3, "n.val" -> 5)
    ))

    // And
    result.graphs shouldBe empty
  }
}
