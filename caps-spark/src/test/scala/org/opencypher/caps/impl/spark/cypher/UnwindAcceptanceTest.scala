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

import org.opencypher.caps.api.value.{CypherList, CypherMap, CypherNode, Properties}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.support.testgraph.{GDLTestGraph, Neo4jTestGraph}

import scala.collection.immutable.Bag

class UnwindAcceptanceTest extends CAPSTestSuite {

  test("standalone unwind from parameter") {
    val query = "UNWIND $param AS item RETURN item"

    val result = caps.cypher(query, Map("param" -> CypherList(Seq(1, 2, 3))))

    result.records.toMapsWithCollectedEntities should equal(Bag(
      CypherMap("item" -> 1),
      CypherMap("item" -> 2),
      CypherMap("item" -> 3)
    ))
  }

  // will work once frontend knows to extract literals from UNWIND
  ignore("standalone unwind from literal") {
    val query = "UNWIND [1, 2, 3] AS item RETURN item"

    val result = caps.cypher(query)

    result.records.toMapsWithCollectedEntities should equal(Bag(
      CypherMap("item" -> 1),
      CypherMap("item" -> 2),
      CypherMap("item" -> 3)
    ))
  }

  test("unwind after match") {
    val graph = GDLTestGraph("(:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

    val query = "MATCH (a)-[r]->(b) UNWIND $param AS item RETURN a, item"

    val result = graph.cypher(query, Map("param" -> CypherList(Seq(1, 2, 3))))

    result.records.toMapsWithCollectedEntities.map(_.toString) should equal(Bag(
      CypherMap("a" -> CypherNode(0L, Seq("A"), Properties.empty), "item" -> 1),
      CypherMap("a" -> CypherNode(0L, Seq("A"), Properties.empty), "item" -> 2),
      CypherMap("a" -> CypherNode(0L, Seq("A"), Properties.empty), "item" -> 3),
      CypherMap("a" -> CypherNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 1),
      CypherMap("a" -> CypherNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 2),
      CypherMap("a" -> CypherNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 3)
    ).map(_.toString))
  }

  test("unwind from expression, aggregation") {
    val graph = GDLTestGraph("(:A {v: 1l}), (:A:B {v: 15l}), (:A:C {v: -32l}), (:A)")

    val query = "MATCH (a:A) WITH collect(a.v) AS list UNWIND list AS item RETURN item"

    val result = graph.cypher(query, Map("param" -> CypherList(Seq(1, 2, 3))))

    result.records.toMapsWithCollectedEntities should equal(Bag(
      CypherMap("item" -> 1),
      CypherMap("item" -> 15),
      CypherMap("item" -> -32)
    ))
  }

  test("unwind from expression") {
    val graph = Neo4jTestGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]}), (:A:C {v: []}), (:A)")

    val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

    val result = graph.cypher(query, Map("param" -> CypherList(Seq(1, 2, 3))))

    result.records.toMapsWithCollectedEntities should equal(Bag(
      CypherMap("item" -> 1),
      CypherMap("item" -> 2),
      CypherMap("item" -> -4)
    ))
  }

  test("unwind in involved query") {
    val graph = GDLTestGraph("(:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

    val query =
      """MATCH (a)-[r]->(b)
        |UNWIND $param AS item
        |WITH a, r, item
        |  WHERE item > 1
        |RETURN a, item""".stripMargin

    val result = graph.cypher(query, Map("param" -> CypherList(Seq(1, 2, 3))))

    result.records.toMapsWithCollectedEntities.map(_.toString) should equal(Bag(
      CypherMap("a" -> CypherNode(0L, Seq("A"), Properties.empty), "item" -> 3),
      CypherMap("a" -> CypherNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 3),
      CypherMap("a" -> CypherNode(0L, Seq("A"), Properties.empty), "item" -> 2),
      CypherMap("a" -> CypherNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 2)
    ).map(_.toString))
  }
}
