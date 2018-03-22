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

import org.opencypher.okapi.api.value.CAPSNode
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.spark.impl.CAPSGraph

trait UnwindBehaviour { self: AcceptanceTest =>

  def unwindBehaviour(initGraph: String => CAPSGraph): Unit = {

    test("standalone unwind from parameter") {
      val query = "UNWIND $param AS item RETURN item"

      val result = caps.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("item" -> 1),
          CypherMap("item" -> 2),
          CypherMap("item" -> 3)
        ))
    }

    test("standalone unwind from literal") {
      val query = "UNWIND [1, 2, 3] AS item RETURN item"

      val result = caps.cypher(query)

      result.getRecords.toMapsWithCollectedEntities should be(
        Bag(
          CypherMap("item" -> 1),
          CypherMap("item" -> 2),
          CypherMap("item" -> 3)
        ))
    }

    test("unwind after match") {
      val graph = initGraph("CREATE (:A)-[:T]->(:B {item: '1'})-[:T]->(:C)")

      val query = "MATCH (a)-[r]->(b) UNWIND $param AS item RETURN a, item"

      val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities.map(_.toString) should equal(
        Bag(
          CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty), "item" -> 1),
          CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty), "item" -> 2),
          CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty), "item" -> 3),
          CypherMap("a" -> CAPSNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 1),
          CypherMap("a" -> CAPSNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 2),
          CypherMap("a" -> CAPSNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 3)
        ).map(_.toString))
    }

    test("unwind from expression, aggregation") {
      val graph = initGraph("CREATE (:A {v: 1}), (:A:B {v: 15}), (:A:C {v: -32}), (:A)")

      val query = "MATCH (a:A) WITH collect(a.v) AS list UNWIND list AS item RETURN item"

      val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("item" -> 1),
          CypherMap("item" -> 15),
          CypherMap("item" -> -32)
        ))
    }

    test("unwind from expression") {
      val graph = initGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]})")

      val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

      val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("item" -> 1),
          CypherMap("item" -> 2),
          CypherMap("item" -> -4)
        ))
    }

    // TODO: activate once https://issues.apache.org/jira/browse/SPARK-23610 is resolved
    ignore("unwind from expression with empty and null lists") {
      val graph = initGraph("CREATE (:A {v: [1, 2]}), (:A:B {v: [-4]}), (:A:C {v: []}), (:A)")

      val query = "MATCH (a:A) WITH a.v AS list UNWIND list AS item RETURN item"

      val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("item" -> 1),
          CypherMap("item" -> 2),
          CypherMap("item" -> -4)
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

      val result = graph.cypher(query, Map("param" -> CypherList(1, 2, 3)))

      result.getRecords.toMapsWithCollectedEntities.map(_.toString) should equal(
        Bag(
          CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty), "item" -> 3),
          CypherMap("a" -> CAPSNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 3),
          CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty), "item" -> 2),
          CypherMap("a" -> CAPSNode(1L, Set("B"), CypherMap("item" -> "1")), "item" -> 2)
        ).map(_.toString))
    }
  }
}
