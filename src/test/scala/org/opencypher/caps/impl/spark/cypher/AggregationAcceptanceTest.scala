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

class AggregationAcceptanceTest extends CAPSTestSuite {

  //--------------------------------------------------------------------------------------------------------------------
  // AVG
  //--------------------------------------------------------------------------------------------------------------------

  test("avg(prop) with integers in WITH") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 4)
    ))
  }

  test("avg(prop) with integers in RETURN") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 4)
    ))
  }

  test("avg(prop) with integers in RETURN without alias") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) RETURN AVG(n.val)")

    result.records.toMaps should equal(Bag(
      CypherMap("AVG(n.val)" -> 4)
    ))
  }

  test("avg(prop) with floats in WITH") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 3.5)
    ))
  }

  test("avg(prop) with floats in RETURN") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 3.5)
    ))
  }

  test("avg(prop) with single null value in WITH") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 32.5)
    ))
  }

  test("avg(prop) with single null value in RETURN") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 32.5)
    ))
  }

  ignore("avg(prop) with only null values in WITH") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  ignore("avg(prop) with only null values in RETURN") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // COUNT
  //--------------------------------------------------------------------------------------------------------------------

  test("count(*) in WITH") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH count(*) AS nbrRows RETURN nbrRows")

    result.records.toMaps should equal(Bag(
      CypherMap("nbrRows" -> 6)
    ))
  }

  test("count(*) in RETURN") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) RETURN count(*) AS nbrRows")

    result.records.toMaps should equal(Bag(
      CypherMap("nbrRows" -> 6)
    ))
  }

  test("count(n) in RETURN") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) RETURN count(n) AS nbrRows")

    result.records.toMaps should equal(Bag(
      CypherMap("nbrRows" -> 6)
    ))
  }

  test("count(n) in RETURN without alias") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) RETURN count(n)")

    result.records.toMaps should equal(Bag(
      CypherMap("count(n)" -> 6)
    ))
  }

  test("count(*) in return without alias") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) RETURN count(*)")

    result.records.toMaps should equal(Bag(
      CypherMap("count(*)" -> 6)
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

  test("count() with grouping") {
    val graph = TestGraph("({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

    val result = graph.cypher("MATCH (n) WITH n.name as name, count(*) AS amount RETURN name, amount")

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "foo", "amount" -> 2),
      CypherMap("name" -> null, "amount" -> 3),
      CypherMap("name" -> "baz", "amount" -> 1)
    ))
  }

  test("count() with grouping on multiple keys") {
    val graph = TestGraph("({name: 'foo', age: 42L}), ({name: 'foo', age: 42L}), ({name: 'foo', age: 23L}), (), (), ({name: 'baz', age: 23L})")

    val result = graph.cypher("MATCH (n) WITH n.name AS name, n.age AS age, count(*) AS amount RETURN name, age, amount")

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "foo", "age" -> 23, "amount" -> 1),
      CypherMap("name" -> "foo", "age" -> 42, "amount" -> 2),
      CypherMap("name" -> "baz", "age" -> 23, "amount" -> 1),
      CypherMap("name" -> null, "age" -> null, "amount" -> 2)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // MIN
  //--------------------------------------------------------------------------------------------------------------------

  test("min(prop) in WITH") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  test("min(prop) in RETURN") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  test("min(prop) with single null value in WITH") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  test("min(prop) with single null value in RETURN") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 23L)
    ))
  }

  test("min(prop) with single null value in RETURN without alias") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) RETURN MIN(n.val)")

    result.records.toMaps should equal(Bag(
      CypherMap("MIN(n.val)" -> 23L)
    ))
  }

  ignore("min(prop) with only null values in WITH") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  ignore("min(prop) with only null values in RETURN") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  //--------------------------------------------------------------------------------------------------------------------
  // MAX
  //--------------------------------------------------------------------------------------------------------------------

  test("max(prop) in WITH") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 84L)
    ))
  }

  test("max(prop) in RETURN") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 84L)
    ))
  }

  test("max(prop) with single null value in WITH") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 42L)
    ))
  }

  test("max(prop) with single null value in RETURN") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 42L)
    ))
  }

  test("max(prop) with single null value in RETURN without alias") {
    val graph = TestGraph("({val:42L}),({val:23L}),()")

    val result = graph.cypher("MATCH (n) RETURN MAX(n.val)")

    result.records.toMaps should equal(Bag(
      CypherMap("MAX(n.val)" -> 42L)
    ))
  }

  ignore("simple max(prop) with only null values in WITH") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  ignore("simple max(prop) with only null values in RETURN") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }
  //--------------------------------------------------------------------------------------------------------------------
  // SUM
  //--------------------------------------------------------------------------------------------------------------------

  test("sum(prop) with integers in WITH") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 12)
    ))
  }

  test("sum(prop) with integers in RETURN") {
    val graph = TestGraph("({val:2L}),({val:4L}),({val:6L})")

    val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 12)
    ))
  }

  test("sum(prop) with floats in WITH") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 10.5)
    ))
  }

  test("sum(prop) with floats in RETURN") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 10.5)
    ))
  }

  test("sum(prop) with floats in RETURN without alias") {
    val graph = TestGraph("({val:5.0D}),({val:5.0D}),({val:0.5D})")

    val result = graph.cypher("MATCH (n) RETURN SUM(n.val)")

    result.records.toMaps should equal(Bag(
      CypherMap("SUM(n.val)" -> 10.5)
    ))
  }

  test("simple sum(prop) with single null value in WITH") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 65.0)
    ))
  }

  test("simple sum(prop) with single null value in RETURN") {
    val graph = TestGraph("({val:42.0D}),({val:23.0D}),()")

    val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 65.0)
    ))
  }

  ignore("simple sum(prop) with only null values in WITH") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }

  ignore("simple sum(prop) with only null values in RETURN") {
    val graph = TestGraph("({val:null}),(),()")

    val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

    result.records.toMaps should equal(Bag(
      CypherMap("res" -> null)
    ))
  }


  //--------------------------------------------------------------------------------------------------------------------
  // Combinations
  //--------------------------------------------------------------------------------------------------------------------

  test("multiple aggregates in WITH") {
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

  test("multiple aggregates in RETURN") {
    val graph = TestGraph("({val:42L}),({val:23L}),({val:84L})")

    val result = graph.cypher(
      """MATCH (n)
        |RETURN
        | AVG(n.val) AS avg,
        | COUNT(*) AS cnt,
        | MIN(n.val) AS min,
        | MAX(n.val) AS max,
        | SUM(n.val) AS sum""".stripMargin)

    result.records.toMaps should equal(Bag(
      CypherMap("avg" -> 49, "cnt" -> 3, "min" -> 23L, "max" -> 84L, "sum" -> 149)
    ))
  }

  test("multiple aggregates with grouping") {
    val graph = TestGraph("({key: 'a', val:42L}),({key: 'a',val:23L}),({key: 'b', val:84L})")

    val result = graph.cypher(
      """MATCH (n)
        |WITH
        | n.key AS key,
        | AVG(n.val) AS avg,
        | COUNT(*) AS cnt,
        | MIN(n.val) AS min,
        | MAX(n.val) AS max,
        | SUM(n.val) AS sum
        |RETURN key, avg, cnt, min, max, sum""".stripMargin)

    result.records.toMaps should equal(Bag(
      CypherMap("key" -> "a", "avg" -> 32, "cnt" -> 2, "min" -> 23L, "max" -> 42L, "sum" -> 65),
      CypherMap("key" -> "b", "avg" -> 84, "cnt" -> 1, "min" -> 84, "max" -> 84, "sum" -> 84)
    ))
  }
}
