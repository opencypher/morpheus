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
 */
package org.opencypher.caps.impl.spark.acceptance

import org.opencypher.caps.api.value.{CAPSList, CAPSMap, CAPSNode, Properties}
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.immutable.Bag

trait UnwindBehaviour { self: AcceptanceTest =>

  def unwindBehaviour(initGraph: String => CAPSGraph): Unit = {

    test("standalone unwind from parameter") {
      val query = "UNWIND $param AS item RETURN item"

      val result = caps.cypher(query, Map("param" -> CAPSList(Seq(1, 2, 3))))

      result.records.toMapsWithCollectedEntities should equal(
        Bag(
          CAPSMap("item" -> 1),
          CAPSMap("item" -> 2),
          CAPSMap("item" -> 3)
        ))
    }

    test("standalone unwind from literal") {
      val query = "UNWIND [1, 2, 3] AS item RETURN item"

      val result = caps.cypher(query)

      result.records.toMapsWithCollectedEntities should equal(
        Bag(
          CAPSMap("item" -> 1),
          CAPSMap("item" -> 2),
          CAPSMap("item" -> 3)
        ))
    }

    test("unwind after match") {
      val graph = initGraph("CREATE (:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

      val query = "MATCH (a)-[r]->(b) UNWIND $param AS item RETURN a, item"

      val result = graph.cypher(query, Map("param" -> CAPSList(Seq(1, 2, 3))))

      result.records.toMapsWithCollectedEntities.map(_.toString) should equal(
        Bag(
          CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty), "item" -> 1),
          CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty), "item" -> 2),
          CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty), "item" -> 3),
          CAPSMap("a" -> CAPSNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 1),
          CAPSMap("a" -> CAPSNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 2),
          CAPSMap("a" -> CAPSNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 3)
        ).map(_.toString))
    }

    test("unwind from expression, aggregation") {
      val graph = initGraph("CREATE (:A {v: 1}), (:A:B {v: 15}), (:A:C {v: -32}), (:A)")

      val query = "MATCH (a:A) WITH collect(a.v) AS list UNWIND list AS item RETURN item"

      val result = graph.cypher(query, Map("param" -> CAPSList(Seq(1, 2, 3))))

      result.records.toMapsWithCollectedEntities should equal(
        Bag(
          CAPSMap("item" -> 1),
          CAPSMap("item" -> 15),
          CAPSMap("item" -> -32)
        ))
    }

    test("unwind from expression") {
      val graph = initGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]}), (:A:C {v: []}), (:A)")

      val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

      val result = graph.cypher(query, Map("param" -> CAPSList(Seq(1, 2, 3))))

      result.records.toMapsWithCollectedEntities should equal(
        Bag(
          CAPSMap("item" -> 1),
          CAPSMap("item" -> 2),
          CAPSMap("item" -> -4)
        ))
    }

    test("unwind in involved query") {
      val graph = initGraph("CREATE (:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

      val query =
        """MATCH (a)-[r]->(b)
        |UNWIND $param AS item
        |WITH a, r, item
        |  WHERE item > 1
        |RETURN a, item""".stripMargin

      val result = graph.cypher(query, Map("param" -> CAPSList(Seq(1, 2, 3))))

      result.records.toMapsWithCollectedEntities.map(_.toString) should equal(
        Bag(
          CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty), "item" -> 3),
          CAPSMap("a" -> CAPSNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 3),
          CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty), "item" -> 2),
          CAPSMap("a" -> CAPSNode(1L, Seq("B"), Properties("item" -> "1")), "item" -> 2)
        ).map(_.toString))
    }
  }
}
