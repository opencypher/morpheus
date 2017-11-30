/*
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
import org.opencypher.caps.demo.Configuration.{DebugPhysicalResult, PrintPhysicalPlan}
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

  test("in with parameter") {
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

  test("float conversion for integer division") {
    // Given
    val given = TestGraph("""(:Node {id: 1L, val: 4L}), (:Node {id: 2L, val: 5L}), (:Node {id: 3L, val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node) WHERE (n.val * 1.0) / n.id >= 2.5 RETURN n.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.id" -> 1),
      CypherMap("n.id" -> 2)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("basic pattern predicate") {
    // Given
    val given = TestGraph("(v {id:1L})-->({id:2L})-->({id:3L})<--(v)")

    // When
    val result = given.cypher("MATCH (a)-->(b) WHERE (a)-->()-->(b) RETURN a.id, b.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L, "b.id" -> 3L)
    ))
  }

  test("pattern predicate with var-length-expand") {
    // Given
    val given = TestGraph("(v {id:1L})-->({id:2L})-->({id:3L})<--(v)")

    // When
    val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[*1..3]->()-->(b) RETURN a.id, b.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L, "b.id" -> 3L)
    ))
  }


  test("simple pattern predicate with node predicate") {
    // Given
    val given = TestGraph(
      """
        |({id:1L})-->({name: 'foo'})
        |({id:3L})-->({name: 'bar'})
      """.stripMargin)

    // When
    val result = given.cypher("MATCH (a) WHERE (a)-->({name: 'foo'}) RETURN a.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L)
    ))
  }

  test("simple pattern predicate with relationship predicate") {
    // Given
    val given = TestGraph(
      """
        |(v{id:1L})-[{val: 'foo'}]->()-->({id:2L})<--(v),
        |(w{id:3L})-[{val: 'bar'}]->()-->({id:4L})<--(w)
      """.stripMargin)

    // When
    val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[{val: 'foo'}]-()-->(b) RETURN a.id, b.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L, "b.id" -> 2L)
    ))
  }

  test("inverse pattern predicate") {
    // Given
    val given = TestGraph("(v {id:1L})-->({id:2L})-->({id:3L})<--(v)")

    // When
    val result = given.cypher("MATCH (a)-->(b) WHERE NOT (a)-->()-->(b) RETURN a.id, b.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L, "b.id" -> 2L),
      CypherMap("a.id" -> 2L, "b.id" -> 3L)
    ))
  }

  test("pattern predicate with derived node predicate") {
    // Given
    val given = TestGraph("({id:1L})-->({id:3L}), ({id:2L})-->({id:3L})")

    // When
    val result = given.cypher("MATCH (a) WHERE (a)-->({val: a.val + 2}) RETURN a.id")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("a.id" -> 1L)
    ))
  }
}
