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

import scala.collection.Bag

class ReturnAcceptanceTest extends CAPSTestSuite {

  test("single return query") {
    val given = TestGraph("")

    val result  = given.cypher("RETURN 1")

    result.records shouldMatch CypherMap("1" -> 1)
  }

  test("single return query with several columns") {
    val given = TestGraph("(), ()")

    val result  = given.cypher("RETURN 1 AS foo, '' AS str")

    result.records shouldMatch CypherMap("foo" -> 1, "str" -> "")
  }

  test("return node") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.records.toMaps should equal(Bag(
      CypherMap("n" -> 0),
      CypherMap("n" -> 1))
    )
  }

  test("return node with details") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("n" -> 0, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> "bar"),
      CypherMap("n" -> 1, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> null))
    )
  }

  test("return rel") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    result.records.toMaps should equal(Bag(
      CypherMap("r" -> 0),
      CypherMap("r" -> 1)
    ))
  }

  test("return rel with details") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()").graph

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    val relId = given.tokens.registry.relTypeRefByName(DEFAULT_LABEL).id

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("r" -> 0, "source(r)" -> 0, "target(r)" -> 1, "type(r)" -> relId, "r.foo" -> "bar"),
      CypherMap("r" -> 1, "source(r)" -> 1, "target(r)" -> 2, "type(r)" -> relId, "r.foo" -> null)
    ))
  }

  test("return distinct properties") {
    val given = TestGraph(
      """({name:'bar'}),
        |({name:'bar'}),
        |({name:'baz'}),
        |({name:'baz'}),
        |({name:'bar'}),
        |({name:'foo'}),
      """.stripMargin)

    val result = given.cypher("MATCH (n) RETURN DISTINCT n.name AS name")

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "bar"),
      CypherMap("name" -> "foo"),
      CypherMap("name" -> "baz")
    ))
  }

  test("return distinct properties for combinations") {
    val given = TestGraph(
      """({p1:'a', p2: 'a', p3: '1'}),
        |({p1:'a', p2: 'a', p3: '2'}),
        |({p1:'a', p2: 'b', p3: '3'}),
        |({p1:'b', p2: 'a', p3: '4'}),
        |({p1:'b', p2: 'b', p3: '5'}),
      """.stripMargin)

    val result = given.cypher("MATCH (n) RETURN DISTINCT n.p1 as p1, n.p2 as p2")

    result.records.toMaps should equal(Bag(
      CypherMap("p1" -> "a", "p2" -> "a"),
      CypherMap("p1" -> "a", "p2" -> "b"),
      CypherMap("p1" -> "b", "p2" -> "a"),
      CypherMap("p1" -> "b", "p2" -> "b")
    ))
  }

}
