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

import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag

class WithAcceptanceTest extends CAPSTestSuite {

  test("rebinding of dropped variables") {
    // Given
    val given = TestGraph("""(:Node {val: 1l}), (:Node {val: 2L})""")

    // When
    val result = given.cypher(
      """MATCH (n:Node)
        |WITH n.val AS foo
        |WITH foo + 2 AS bar
        |WITH bar + 2 AS foo
        |RETURN foo
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("foo" -> 5),
      CypherMap("foo" -> 6)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting constants") {
    // Given
    val given = TestGraph("""(), ()""")

    // When
    val result = given.cypher(
      """MATCH ()
        |WITH 3 AS foo
        |WITH foo + 2 AS bar
        |RETURN bar
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("bar" -> 5),
      CypherMap("bar" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting variables in scope") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n, m RETURN n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting property expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val AS n_val RETURN n_val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n_val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting property expression with filter") {

    // Given
    val given = TestGraph("""(:Node {val: 3L}), (:Node {val: 4L}), (:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node) WITH n.val AS n_val WHERE n_val <= 4 RETURN n_val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n_val" -> 3),
      CypherMap("n_val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting addition expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val + m.val AS sum_n_m_val RETURN sum_n_m_val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("sum_n_m_val" -> 9)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("aliasing variables") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val + m.val AS sum WITH sum AS sum2 RETURN sum2")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("sum2" -> 9)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting mixed expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})-->(:Node)""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val AS n_val, n.val + m.val AS sum_n_m_val RETURN sum_n_m_val, n_val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("sum_n_m_val" -> 9, "n_val" -> 4),
      CypherMap("sum_n_m_val" -> null, "n_val" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by asc") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val ASC RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by desc") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val DESC RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 42L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 3L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val SKIP 2 RETURN val")

    // Then
    result.records.toDF().count() should equal(1)

    // And
    result.graph shouldMatch given.graph
  }

  test("order by with skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by with (arithmetic) skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 + 1 RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 42L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val LIMIT 1 RETURN val")

    // Then
    result.records.toDF().count() should equal(1)

    // And
    result.graph shouldMatch given.graph
  }

  test("order by with limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by with (arithmetic) limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 + 1 RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("order by with skip and limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 LIMIT 1 RETURN val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 4L)
    ))

    // And
    result.graph shouldMatch given.graph
  }
}
