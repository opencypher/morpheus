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

import org.opencypher.caps.api.value.{CypherList, CypherMap}
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.immutable.Bag

class FunctionsAcceptanceTest extends CAPSTestSuite {

  test("exists()") {
    val given = TestGraph("({id: 1L}), ({id: 2L}), ({other: 'foo'}), ()")

    val result = given.cypher("MATCH (n) RETURN exists(n.id) AS exists")

    result.records.toMaps should equal(Bag(
      CypherMap("exists" -> true),
      CypherMap("exists" -> true),
      CypherMap("exists" -> false),
      CypherMap("exists" -> false)
    ))
  }

  test("type()") {
    val given = TestGraph("()-[:KNOWS]->()-[:HATES]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN type(r)")

    result.records.toMaps should equal(Bag(
      CypherMap("type(r)" -> "KNOWS"),
      CypherMap("type(r)" -> "HATES")
    ))
  }

  test("id for node") {
    val given = TestGraph("(),()")

    val result = given.cypher("MATCH (n) RETURN id(n)")

    result.records.toMaps should equal(Bag(
      CypherMap("id(n)" -> 0),
      CypherMap("id(n)" -> 1))
    )

    result.graphs shouldBe empty
  }

  test("id for rel") {
    val given = TestGraph("()-->()-->()")

    val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

    result.records.toMaps should equal(Bag(
      CypherMap("id(e)" -> 0),
      CypherMap("id(e)" -> 1))
    )

    result.graphs shouldBe empty
  }

  test("get single label") {
    val given = TestGraph("(:A)(:B)")

    val result = given.cypher("MATCH (a) RETURN labels(a)")

    result.records.toMaps should equal(Bag(
      CypherMap("labels(a)" -> cypherList(IndexedSeq("A"))),
      CypherMap("labels(a)" -> cypherList(IndexedSeq("B")))
    ))
  }

  test("get multiple labels") {
    val given = TestGraph("(:A:B)(:C:D)")

    val result = given.cypher("MATCH (a) RETURN labels(a)")

    result.records.toMaps should equal(Bag(
      CypherMap("labels(a)" -> cypherList(IndexedSeq("A","B"))),
      CypherMap("labels(a)" -> cypherList(IndexedSeq("C","D")))
    ))
  }

  ignore("unlabeled nodes") {
    // TODO: Support unlabeled nodes in GDL
    val given = TestGraph("(:A), (:C:D), ()")

    val result = given.cypher("MATCH (a) RETURN labels(a)")

    result.records.toMaps should equal(Bag(
      CypherMap("labels(a)" -> cypherList(IndexedSeq("A"))),
      CypherMap("labels(a)" -> cypherList(IndexedSeq("C","D"))),
      CypherMap("labels(a)" -> CypherList.empty)
    ))
  }

  test("size() on literal list") {
    val given = TestGraph("()")

    val result = given.cypher("MATCH () RETURN size(['Alice', 'Bob']) as s")

    result.records.toMaps should equal(Bag(
      CypherMap("s" -> 2)
    ))
  }

  test("size() on literal string") {
    val given = TestGraph("()")

    val result = given.cypher("MATCH () RETURN size('Alice') as s")

    result.records.toMaps should equal(Bag(
      CypherMap("s" -> 5)
    ))
  }

  test("size() on retrieved string") {
    val given = TestGraph("({name: 'Alice'})")

    val result = given.cypher("MATCH (a) RETURN size(a.name) as s")

    result.records.toMaps should equal(Bag(
      CypherMap("s" -> 5)
    ))
  }

  test("size() on constructed list") {
    val given = TestGraph("(:A:B), (:C:D), (:A), ()")

    val result = given.cypher("MATCH (a) RETURN size(labels(a)) as s")

    println(result.records.toMaps)
    result.records.toMaps should equal(Bag(
      CypherMap("s" -> 2),
      CypherMap("s" -> 2),
      CypherMap("s" -> 1),
      CypherMap("s" -> 1) // TODO: GDL does not support nodes without label -- has default here
    ))
  }

  ignore("size() on null") {
    val given = TestGraph("()")

    val result = given.cypher("MATCH (a) RETURN size(a.prop) as s")

    println(result.records.toMaps)
    result.records.toMaps should equal(Bag(
      CypherMap("s" -> null)))
  }

  test("keys()") {
    val given = TestGraph("""({name:'Alice', age:64L, eyes:'brown'})""")

    val result = given.cypher("MATCH (a) WHERE a.name = 'Alice' RETURN keys(a) as k")

    val keysAsMap = result.records.toMaps

    keysAsMap should equal(Bag(
      CypherMap("k" -> CypherList(Seq("age", "eyes", "name")))
    ))
  }

  test("keys() does not return keys of unset properties") {
    val given = TestGraph(
      """(:Person {name:'Alice', age:64L, eyes:'brown'}),
        | (:Person {name:'Bob', eyes:'blue'})""".stripMargin)

    val result = given.cypher("MATCH (a: Person) WHERE a.name = 'Bob' RETURN keys(a) as k")

    result.records.toMaps should equal(Bag(
      CypherMap("k" -> CypherList(Seq("eyes", "name")))
    ))
  }

  // TODO: Enable when "Some error in type inference: Don't know how to type MapExpression" is fixed
  ignore("keys() works with literal maps") {
    val given = TestGraph("()")

    val result = given.cypher("MATCH () WITH {person: {name: 'Anne', age: 25}} AS p RETURN keys(p) as k")

    result.records.toMaps should equal(Bag(
      CypherMap("k" -> CypherList(Seq("age", "name")))
    ))
  }

}
