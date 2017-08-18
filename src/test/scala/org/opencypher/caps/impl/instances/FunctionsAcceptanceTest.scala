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
package org.opencypher.caps.impl.instances

import org.opencypher.caps.SparkCypherTestSuite
import org.opencypher.caps.api.value.CypherMap

import scala.collection.immutable.Bag

class FunctionsAcceptanceTest extends SparkCypherTestSuite {

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
}
