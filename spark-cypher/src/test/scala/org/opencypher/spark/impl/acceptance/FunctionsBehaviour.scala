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
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class FunctionsBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("exists") {

    it("exists()") {
      val given = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

      val result = given.cypher("MATCH (n) RETURN exists(n.id) AS exists")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("exists" -> true),
          CypherMap("exists" -> true),
          CypherMap("exists" -> false),
          CypherMap("exists" -> false)
        ))
    }
  }

  describe("type") {

    test("type()") {
      val given = initGraph("CREATE ()-[:KNOWS]->()-[:HATES]->()-[:REL]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN type(r)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("type(r)" -> "KNOWS"),
          CypherMap("type(r)" -> "HATES"),
          CypherMap("type(r)" -> "REL")
        ))
    }
  }

  describe("id") {

    test("id for node") {
      val given = initGraph("CREATE (),()")

      val result = given.cypher("MATCH (n) RETURN id(n)")

      result.getRecords.toMaps should equal(Bag(CypherMap("id(n)" -> 0), CypherMap("id(n)" -> 1)))
    }

    test("id for rel") {
      val given = initGraph("CREATE ()-[:REL]->()-[:REL]->()")

      val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

      result.getRecords.toMaps should equal(Bag(CypherMap("id(e)" -> 2), CypherMap("id(e)" -> 4)))
    }

  }

  describe("labels") {

    test("get single label") {
      val given = initGraph("CREATE (:A), (:B)")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A")),
          CypherMap("labels(a)" -> List("B"))
        ))
    }

    test("get multiple labels") {
      val given = initGraph("CREATE (:A:B), (:C:D)")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A", "B")),
          CypherMap("labels(a)" -> List("C", "D"))
        ))
    }

    test("unlabeled nodes") {
      val given = initGraph("CREATE (:A), (:C:D), ()")

      val result = given.cypher("MATCH (a) RETURN labels(a)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("labels(a)" -> List("A")),
          CypherMap("labels(a)" -> List("C", "D")),
          CypherMap("labels(a)" -> List.empty)
        ))
    }

  }

  describe("size") {

    test("size() on literal list") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () RETURN size(['Alice', 'Bob']) as s")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("s" -> 2)
        ))
    }

    test("size() on literal string") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () RETURN size('Alice') as s")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("s" -> 5)
        ))
    }

    test("size() on retrieved string") {
      val given = initGraph("CREATE ({name: 'Alice'})")

      val result = given.cypher("MATCH (a) RETURN size(a.name) as s")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("s" -> 5)
        ))
    }

    test("size() on constructed list") {
      val given = initGraph("CREATE (:A:B), (:C:D), (:A), ()")

      val result = given.cypher("MATCH (a) RETURN size(labels(a)) as s")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("s" -> 2),
          CypherMap("s" -> 2),
          CypherMap("s" -> 1),
          CypherMap("s" -> 0)
        ))
    }

    ignore("size() on null") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH (a) RETURN size(a.prop) as s")

      result.getRecords.toMaps should equal(Bag(CypherMap("s" -> null)))
    }

  }

  describe("keys") {

    test("keys()") {
      val given = initGraph("CREATE ({name:'Alice', age: 64, eyes:'brown'})")

      val result = given.cypher("MATCH (a) WHERE a.name = 'Alice' RETURN keys(a) as k")

      val keysAsMap = result.getRecords.toMaps

      keysAsMap should equal(
        Bag(
          CypherMap("k" -> List("age", "eyes", "name"))
        ))
    }

    test("keys() does not return keys of unset properties") {
      val given = initGraph(
        """
          |CREATE (:Person {name:'Alice', age: 64, eyes:'brown'})
          |CREATE (:Person {name:'Bob', eyes:'blue'})
        """.stripMargin)

      val result = given.cypher("MATCH (a: Person) WHERE a.name = 'Bob' RETURN keys(a) as k")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("k" -> List("eyes", "name"))
        ))
    }

    // TODO: Enable when "Some error in type inference: Don't know how to type MapExpression" is fixed
    ignore("keys() works with literal maps") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("MATCH () WITH {person: {name: 'Anne', age: 25}} AS p RETURN keys(p) as k")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("k" -> List("age", "name"))
        ))
    }

  }

  describe("startNode") {

    test("startNode()") {
      val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

      val result = given.cypher("MATCH ()-[r:FOO]->() RETURN r.val, startNode(r)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("r.val" -> "a", "startNode(r)" -> 0),
          CypherMap("r.val" -> "b", "startNode(r)" -> 3)
        ))
    }
  }

  describe("endNode") {

    test("endNode()") {
      val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

      val result = given.cypher("MATCH (a)-[r]->() RETURN r.val, endNode(r)")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("r.val" -> "a", "endNode(r)" -> 1),
          CypherMap("r.val" -> "b", "endNode(r)" -> 4)
        ))
    }
  }

  describe("toFloat") {

    test("toFloat from integers") {
      val given = initGraph("CREATE (a {val: 1})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 1.0)
        ))
    }

    test("toFloat from float") {
      val given = initGraph("CREATE (a {val: 1.0d})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 1.0)
        ))
    }

    test("toFloat from string") {
      val given = initGraph("CREATE (a {val: '42'})")

      val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

      result.getRecords.toMaps should equal(
        Bag(
          CypherMap("myFloat" -> 42.0)
        ))
    }
  }

  describe("coalesce") {
    it("can evaluate coalesce") {
      val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ({valC: 3}), ()")

      val result = given.cypher("MATCH (n) RETURN coalesce(n.valA, n.valB, n.valC) as value")

      result.getRecords.collect.toBag should equal(
        Bag(
          CypherMap("value" -> 1),
          CypherMap("value" -> 2),
          CypherMap("value" -> 3),
          CypherMap("value" -> null)
        ))
    }

    it("can evaluate coalesce on non-existing expressions") {
      val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ()")

      val result = given.cypher("MATCH (n) RETURN coalesce(n.valD, n.valE) as value")

      result.getRecords.collect.toBag should equal(
        Bag(
          CypherMap("value" -> null),
          CypherMap("value" -> null),
          CypherMap("value" -> null)
        ))
    }

  }
}
