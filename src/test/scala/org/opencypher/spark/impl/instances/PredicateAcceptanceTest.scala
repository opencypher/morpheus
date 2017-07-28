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
package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

import scala.collection.Bag

class PredicateAcceptanceTest extends SparkCypherTestSuite {

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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
  }
}
