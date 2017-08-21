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

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.value.CypherMap

import scala.collection.immutable.Bag

class AggregationAcceptanceTest extends CAPSTestSuite {

  //--------------------------------------------------------------------------------------------------------------------
  // COUNT
  //--------------------------------------------------------------------------------------------------------------------

  test("simple count(*)") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH count(*) AS nbrRows RETURN nbrRows")

    result.records.toMaps should equal(Bag(
      CypherMap("nbrRows" -> 6)
    ))
  }

  test("simple count(prop)") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH count(n.name) AS nonNullNames RETURN nonNullNames")

    result.records.toMaps should equal(Bag(
      CypherMap("nonNullNames" -> 3)
    ))
  }

  test("simple count(node)") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH count(n) AS nodes RETURN nodes")

    result.records.toMaps should equal(Bag(
      CypherMap("nodes" -> 6)
    ))
  }

  test("count after expand") {
    val graph = TestGraph("({name: 'foo'})-->(:B), ({name: 'bar'}), (), ()-->(:B), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n)-->(b:B) WITH count(b) AS nodes RETURN nodes")

    result.records.toMaps should equal(Bag(
      CypherMap("nodes" -> 2)
    ))
  }

  ignore("count() with grouping") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH n.name, count(*) AS amount RETURN n.name, amount")

    result.records.toMaps should equal(Bag(
      CypherMap("n.name" -> "foo", "amount" -> 2),
      CypherMap("n.name" -> null, "amount" -> 3),
      CypherMap("n.name" -> "baz", "amount" -> 1)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // MIN
  //--------------------------------------------------------------------------------------------------------------------

  test("simple min(prop)") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  test("simple min(prop) with single null value") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  ignore("simple min(prop) with only null values") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // Combinations
  //--------------------------------------------------------------------------------------------------------------------


  test("multiple aggregates") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS min, COUNT(*) AS cnt RETURN min, cnt")

    result.records.toMaps should equal(Bag(
      CypherMap("min" -> 23L, "cnt" -> 3)
    ))
  }
}
