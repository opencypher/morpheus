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

import org.opencypher.caps.api.value.{CAPSList, CAPSMap}
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.immutable.Bag

trait FunctionsBehaviour {
  self: AcceptanceTest =>

  def functionsBehaviour(initGraph: String => CAPSGraph): Unit = {

    describe("exists") {

      test("exists()") {
        val given = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

        val result = given.cypher("MATCH (n) RETURN exists(n.id) AS exists")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("exists" -> true),
            CAPSMap("exists" -> true),
            CAPSMap("exists" -> false),
            CAPSMap("exists" -> false)
          ))
      }
    }

    describe("type") {

      test("type()") {
        val given = initGraph("CREATE ()-[:KNOWS]->()-[:HATES]->()-[:REL]->()")

        val result = given.cypher("MATCH ()-[r]->() RETURN type(r)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("type(r)" -> "KNOWS"),
            CAPSMap("type(r)" -> "HATES"),
            CAPSMap("type(r)" -> "REL")
          ))
      }
    }

    describe("id") {

      test("id for node") {
        val given = initGraph("CREATE (),()")

        val result = given.cypher("MATCH (n) RETURN id(n)")

        result.records.toMaps should equal(Bag(CAPSMap("id(n)" -> 0), CAPSMap("id(n)" -> 1)))

        result.graphs shouldBe empty
      }

      test("id for rel") {
        val given = initGraph("CREATE ()-[:REL]->()-[:REL]->()")

        val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

        result.records.toMaps should equal(Bag(CAPSMap("id(e)" -> 2), CAPSMap("id(e)" -> 4)))

        result.graphs shouldBe empty
      }

    }

    describe("labels") {

      test("get single label") {
        val given = initGraph("CREATE (:A), (:B)")

        val result = given.cypher("MATCH (a) RETURN labels(a)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("A"))),
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("B")))
          ))
      }

      test("get multiple labels") {
        val given = initGraph("CREATE (:A:B), (:C:D)")

        val result = given.cypher("MATCH (a) RETURN labels(a)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("A", "B"))),
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("C", "D")))
          ))
      }

      test("unlabeled nodes") {
        val given = initGraph("CREATE (:A), (:C:D), ()")

        val result = given.cypher("MATCH (a) RETURN labels(a)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("A"))),
            CAPSMap("labels(a)" -> cypherList(IndexedSeq("C", "D"))),
            CAPSMap("labels(a)" -> CAPSList.empty)
          ))
      }

    }

    describe("size") {

      test("size() on literal list") {
        val given = initGraph("CREATE ()")

        val result = given.cypher("MATCH () RETURN size(['Alice', 'Bob']) as s")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("s" -> 2)
          ))
      }

      test("size() on literal string") {
        val given = initGraph("CREATE ()")

        val result = given.cypher("MATCH () RETURN size('Alice') as s")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("s" -> 5)
          ))
      }

      test("size() on retrieved string") {
        val given = initGraph("CREATE ({name: 'Alice'})")

        val result = given.cypher("MATCH (a) RETURN size(a.name) as s")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("s" -> 5)
          ))
      }

      test("size() on constructed list") {
        val given = initGraph("CREATE (:A:B), (:C:D), (:A), ()")

        val result = given.cypher("MATCH (a) RETURN size(labels(a)) as s")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("s" -> 2),
            CAPSMap("s" -> 2),
            CAPSMap("s" -> 1),
            CAPSMap("s" -> 0)
          ))
      }

      ignore("size() on null") {
        val given = initGraph("CREATE ()")

        val result = given.cypher("MATCH (a) RETURN size(a.prop) as s")

        result.records.toMaps should equal(Bag(CAPSMap("s" -> null)))
      }

    }

    describe("keys") {

      test("keys()") {
        val given = initGraph("CREATE ({name:'Alice', age: 64, eyes:'brown'})")

        val result = given.cypher("MATCH (a) WHERE a.name = 'Alice' RETURN keys(a) as k")

        val keysAsMap = result.records.toMaps

        keysAsMap should equal(
          Bag(
            CAPSMap("k" -> CAPSList(Seq("age", "eyes", "name")))
          ))
      }

      test("keys() does not return keys of unset properties") {
        val given = initGraph(
          """
            |CREATE (:Person {name:'Alice', age: 64, eyes:'brown'})
            |CREATE (:Person {name:'Bob', eyes:'blue'})
          """.stripMargin)

        val result = given.cypher("MATCH (a: Person) WHERE a.name = 'Bob' RETURN keys(a) as k")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("k" -> CAPSList(Seq("eyes", "name")))
          ))
      }

      // TODO: Enable when "Some error in type inference: Don't know how to type MapExpression" is fixed
      ignore("keys() works with literal maps") {
        val given = initGraph("CREATE ()")

        val result = given.cypher("MATCH () WITH {person: {name: 'Anne', age: 25}} AS p RETURN keys(p) as k")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("k" -> CAPSList(Seq("age", "name")))
          ))
      }

    }

    describe("startNode") {

      test("startNode()") {
        val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

        val result = given.cypher("MATCH ()-[r:FOO]->() RETURN r.val, startNode(r)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("r.val" -> "a", "startNode(r)" -> 0),
            CAPSMap("r.val" -> "b", "startNode(r)" -> 3)
          ))
      }
    }

    describe("endNode") {

      test("endNode()") {
        val given = initGraph("CREATE ()-[:FOO {val: 'a'}]->(),()-[:FOO {val: 'b'}]->()")

        val result = given.cypher("MATCH (a)-[r]->() RETURN r.val, endNode(r)")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("r.val" -> "a", "endNode(r)" -> 1),
            CAPSMap("r.val" -> "b", "endNode(r)" -> 4)
          ))
      }
    }

    describe("toFloat") {

      test("toFloat from integers") {
        val given = initGraph("CREATE (a {val: 1})")

        val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("myFloat" -> 1.0)
          ))
      }

      test("toFloat from float") {
        val given = initGraph("CREATE (a {val: 1.0d})")

        val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("myFloat" -> 1.0)
          ))
      }

      test("toFloat from string") {
        val given = initGraph("CREATE (a {val: '42'})")

        val result = given.cypher("MATCH (a) RETURN toFloat(a.val) as myFloat")

        result.records.toMaps should equal(
          Bag(
            CAPSMap("myFloat" -> 42.0)
          ))
      }
    }

    describe("coalesce") {
      it("can evaluate coalesce") {
        val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ({valC: 3}), ()")

        val result = given.cypher("MATCH (n) RETURN coalesce(n.valA, n.valB, n.valC) as value")

        result.records.iterator.toBag should equal(
          Bag(
            CAPSMap("value" -> 1),
            CAPSMap("value" -> 2),
            CAPSMap("value" -> 3),
            CAPSMap("value" -> null)
          ))
      }

      it("can evaluate coalesce on non-existing expressions") {
        val given = initGraph("CREATE ({valA: 1}), ({valB: 2}), ()")

        val result = given.cypher("MATCH (n) RETURN coalesce(n.valD, n.valE) as value")

        result.records.iterator.toBag should equal(
          Bag(
            CAPSMap("value" -> null),
            CAPSMap("value" -> null),
            CAPSMap("value" -> null)
          ))
      }

    }
  }
}
