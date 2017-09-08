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

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.value.CypherMap

import scala.collection.immutable.Bag

class AggregationAcceptanceTest extends CAPSTestSuite {

  //--------------------------------------------------------------------------------------------------------------------
  // AVG
  //--------------------------------------------------------------------------------------------------------------------

  test("simple avg(prop) with integers") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 4)
    ))
  }

  test("simple avg(prop) with floats") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 3.5)
    ))
  }

  test("simple avg(prop) with single null value") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 32.5)
    ))
  }

  ignore("simple avg(prop) with only null values") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

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
  // MAX
  //--------------------------------------------------------------------------------------------------------------------

  test("simple max(prop)") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 84L)
    ))
  }

  test("simple max(prop) with single null value") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 42L)
    ))
  }

  ignore("simple max(prop) with only null values") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }
  //--------------------------------------------------------------------------------------------------------------------
  // SUM
  //--------------------------------------------------------------------------------------------------------------------

  test("simple sum(prop) with integers") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 12)
    ))
  }

  test("simple sum(prop) with floats") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 10.5)
    ))
  }

  test("simple sum(prop) with single null value") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 65.0)
    ))
  }

  ignore("simple sum(prop) with only null values") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // Combinations
  //--------------------------------------------------------------------------------------------------------------------

  test("multiple aggregates") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher(
      """MATCH (n)
        |WITH
        | AVG(n.val) AS avg,
        | COUNT(*) AS cnt,
        | MIN(n.val) AS min,
        | MAX(n.val) AS max,
        | SUM(n.val) AS sum
        |RETURN avg, cnt, min, max, sum""".stripMargin)

    result.records.toMaps should equal(Bag(
      CypherMap("avg" -> 49, "cnt" -> 3, "min" -> 23L, "max" -> 84L, "sum" -> 149)
    ))
  }
}
