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

import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class WithBehaviour extends CAPSTestSuite with DefaultGraphInit {

  test("rebinding of dropped variables") {
    // Given
    val given = initGraph("""CREATE (:Node {val: 1}), (:Node {val: 2})""")

    // When
    val result = given.cypher(
      """MATCH (n:Node)
        |WITH n.val AS foo
        |WITH foo + 2 AS bar
        |WITH bar + 2 AS foo
        |RETURN foo
      """.stripMargin)

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("foo" -> 5),
        CypherMap("foo" -> 6)
      ))
  }

  test("projecting constants") {
    // Given
    val given = initGraph("""CREATE (), ()""")

    // When
    val result = given.cypher(
      """MATCH ()
        |WITH 3 AS foo
        |WITH foo + 2 AS bar
        |RETURN bar
      """.stripMargin)

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("bar" -> 5),
        CypherMap("bar" -> 5)
      ))
  }

  test("projecting variables in scope") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n, m RETURN n.val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("n.val" -> 4)
      ))
  }

  test("projecting property expression") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val AS n_val RETURN n_val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("n_val" -> 4)
      ))
  }

  test("projecting property expression with filter") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 3}), (:Node {val: 4}), (:Node {val: 5})""")

    // When
    val result = given.cypher("MATCH (n:Node) WITH n.val AS n_val WHERE n_val <= 4 RETURN n_val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("n_val" -> 3),
        CypherMap("n_val" -> 4)
      ))
  }

  test("projecting addition expression") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val + m.val AS sum_n_m_val RETURN sum_n_m_val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("sum_n_m_val" -> 9)
      ))
  }

  test("aliasing variables") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val + m.val AS sum WITH sum AS sum2 RETURN sum2")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("sum2" -> 9)
      ))
  }

  test("projecting mixed expression") {

    // Given
    val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})-[:Rel]->(:Node)""")

    // When
    val result = given.cypher(
      "MATCH (n:Node)-[r]->(m:Node) WITH n.val AS n_val, n.val + m.val AS sum_n_m_val RETURN sum_n_m_val, n_val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("sum_n_m_val" -> 9, "n_val" -> 4),
        CypherMap("sum_n_m_val" -> null, "n_val" -> 5)
      ))
  }

  it("can project and predicates") {
    val graph = initGraph(
      """
        |CREATE ({val1: 1, val2: 3, val3: 10}), ({val1: 1, val2: 2, val3: 3})
      """.stripMargin)

    val result = graph.cypher(
      """
        |MATCH (n)
        |WITH n.val1 AS val1, n.val2 AS val2, n.val3 AS val3
        |WHERE val1 >= 1 AND val2 > 2 AND val3 > 5
        |RETURN val1, val2, val3
      """.stripMargin)

    result.getRecords.collect.toBag should equal(Bag(
      CypherMap("val1" -> 1, "val2" -> 3, "val3" -> 10)
    ))
  }

  test("order by") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
  }

  test("order by asc") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val ASC RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
  }

  test("order by desc") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val DESC RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 42L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 3L)
      ))
  }

  test("skip") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val SKIP 2 RETURN val").asCaps

    // Then
    result.getRecords.toDF().count() should equal(1)
  }

  test("order by with skip") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
  }

  test("order by with (arithmetic) skip") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 + 1 RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 42L)
      ))
  }

  test("limit") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val LIMIT 1 RETURN val").asCaps

    // Then
    result.getRecords.toDF().count() should equal(1)
  }

  test("order by with limit") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 3L)
      ))
  }

  test("order by with (arithmetic) limit") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 + 1 RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L)
      ))
  }

  test("order by with skip and limit") {
    val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

    val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 LIMIT 1 RETURN val")

    // Then
    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("val" -> 4L)
      ))
  }


  describe("NOT") {
    it("can project not of literal") {
      // Given
      val given = initGraph(
        """
          |CREATE ()
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |WITH true AS t, false AS f
          |WITH NOT true AS nt, not false AS nf
          |RETURN nt, nf""".stripMargin)

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("nt" -> false, "nf" -> true)
      ))
    }

    it("can project not of expression") {
      // Given
      val given = initGraph(
        """
          |CREATE ({id: 1, val: true}), ({id: 2, val: false})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (n)
          |WITH n.id AS id, NOT n.val AS val2
          |RETURN id, val2""".stripMargin)

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("id" -> 1L, "val2" -> false),
        CypherMap("id" -> 2L, "val2" -> true)
      ))
    }
  }
}
