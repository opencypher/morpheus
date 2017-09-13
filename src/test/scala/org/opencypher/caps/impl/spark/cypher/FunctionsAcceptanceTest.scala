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

import scala.collection.immutable.Bag

class FunctionsAcceptanceTest extends CAPSTestSuite {

  test("exists()") {
    val given = TestGraph("({id: 1l}), ({id: 2l}), ({other: 'foo'}), ()")

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

    result.graph shouldMatch given.graph
  }

  test("id for rel") {
    val given = TestGraph("()-->()-->()")

    val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

    result.records.toMaps should equal(Bag(
      CypherMap("id(e)" -> 0),
      CypherMap("id(e)" -> 1))
    )

    result.graph shouldMatch given.graph
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

  // TODO: Need help: CAPSException: Did not find slot for labels(a :: NODE)
  test("size() on constructed list") {
    val given = TestGraph("(:A:B)(:C:D)")

    val result = given.cypher("MATCH (a) RETURN size(labels(a)) as s")

    println(result.records.toMaps)
    result.records.toMaps should equal(Bag(
      CypherMap("s" -> 2),
      CypherMap("s" -> 2)
    ))
  }

  test("size() on null") {
    val given = TestGraph("()")

    val result = given.cypher("MATCH (a: NonExistent) RETURN size(labels(a)) as s")

    println(result.records.toMaps)
    result.records.toMaps should equal(Bag(
      CypherMap("s" -> null)
    ))
  }

}
