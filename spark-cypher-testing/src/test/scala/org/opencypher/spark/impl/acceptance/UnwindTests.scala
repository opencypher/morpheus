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

import org.opencypher.okapi.api.value.CypherValue.{CypherMap, _}
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.value.MorpheusNode
import org.opencypher.spark.testing.MorpheusTestSuite

class UnwindTests extends MorpheusTestSuite with ScanGraphInit {

  it("standalone unwind from parameter") {
    val query = "UNWIND $param AS item RETURN item"

    val result = morpheus.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps should equal(
      Bag(
        CypherMap("item" -> 1),
        CypherMap("item" -> 2),
        CypherMap("item" -> 3)
      ))
  }

  it("standalone unwind from literal") {
    val query = "UNWIND [1, 2, 3] AS item RETURN item"

    val result = morpheus.cypher(query)

    result.records.toMaps should be(
      Bag(
        CypherMap("item" -> 1),
        CypherMap("item" -> 2),
        CypherMap("item" -> 3)
      ))
  }

  it("unwind after match") {
    val graph = initGraph("CREATE (:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

    val query = "MATCH (a)-[r]->(b) UNWIND $param AS item RETURN a, item"

    val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps.map(_.toString) should equal(
      Bag(
        CypherMap("a" -> MorpheusNode(0L, Set("A"), CypherMap.empty), "item" -> 1),
        CypherMap("a" -> MorpheusNode(0L, Set("A"), CypherMap.empty), "item" -> 2),
        CypherMap("a" -> MorpheusNode(0L, Set("A"), CypherMap.empty), "item" -> 3),
        CypherMap("a" -> MorpheusNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 1),
        CypherMap("a" -> MorpheusNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 2),
        CypherMap("a" -> MorpheusNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 3)
      ).map(_.toString))
  }

  it("unwind from expression, aggregation") {
    val graph = initGraph("CREATE (:A {v: 1}), (:A:B {v: 15}), (:A:C {v: -32}), (:A)")

    val query = "MATCH (a:A) WITH collect(a.v) AS list UNWIND list AS item RETURN item"

    val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps should equal(
      Bag(
        CypherMap("item" -> 1),
        CypherMap("item" -> 15),
        CypherMap("item" -> -32)
      ))
  }

  it("unwind from expression") {
    val graph = initGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]})")

    val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

    val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps should equal(
      Bag(
        CypherMap("item" -> 1),
        CypherMap("item" -> 2),
        CypherMap("item" -> -4)
      ))
  }

  // https://issues.apache.org/jira/browse/SPARK-23610
  // TODO: Issue is that empty lists are Array(String) in Spark
  // We lack a way of encoding empty lists in a way that expands to a specific list type in Spark
  // Like putting it in a column with Array(Integer) as in this test
  ignore("unwind from expression with empty and null lists") {
    val graph = initGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]}), (:A:C {v: []}), (:A)")

    val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

    val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps should equal(
      Bag(
        CypherMap("item" -> 1),
        CypherMap("item" -> 2),
        CypherMap("item" -> -4)
      ))
  }

  it("unwind from literal null expression") {
    val graph = initGraph("CREATE (:A)")

    val query =
      """
        |UNWIND null AS item
        |RETURN item""".stripMargin

    val result = graph.cypher(query)

    result.records.toMaps should be(empty)
  }

  it("unwind from null expression") {
    val graph = initGraph("CREATE (:A)")

    val query =
      """
        |MATCH (a:A)
        |WITH a.v AS list, a
        |UNWIND list AS item
        |RETURN a, item""".stripMargin

    val result = graph.cypher(query)

    result.records.toMaps should be(empty)
  }

  it("unwinds in an involved query") {
    val graph = initGraph("CREATE (:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

    val query =
      """MATCH (a)-[r]->(b)
        |UNWIND $param AS item
        |WITH a, r, item
        |  WHERE item > 1
        |RETURN a, item""".stripMargin

    val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

    result.records.toMaps should equal(
      Bag(
        CypherMap("a" -> MorpheusNode(0L, Set("A"), CypherMap.empty), "item" -> 3),
        CypherMap("a" -> MorpheusNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 3),
        CypherMap("a" -> MorpheusNode(0L, Set("A"), CypherMap.empty), "item" -> 2),
        CypherMap("a" -> MorpheusNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 2)
      )
    )
  }
}
