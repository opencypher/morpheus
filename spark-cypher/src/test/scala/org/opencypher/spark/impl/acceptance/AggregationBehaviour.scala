/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class AggregationBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("AVG") {

    test("avg(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val:2}),({val:4}),({val:6})")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.getRecords.collect.toBag should equal(Bag(
        CypherMap("res" -> 4)
      ))
    }

    test("avg(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.getRecords.collect.toBag should equal(Bag(
        CypherMap("res" -> 4)
      ))
    }

    test("avg(prop) with integers in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("AVG(n.val)" -> 4)
      ))
    }

    test("avg(prop) with floats in WITH") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 3.5)
      ))
    }

    test("avg(prop) with floats in RETURN") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 3.5)
      ))
    }

    test("avg(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 32.5)
      ))
    }

    test("avg(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 32.5)
      ))
    }

    ignore("avg(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("avg(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }

  describe("COUNT") {

    test("count(*) in WITH") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(*) AS nbrRows RETURN nbrRows")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    test("count(*) in RETURN") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(*) AS nbrRows")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    test("count(n) in RETURN") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(n) AS nbrRows")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    test("count(n) in RETURN without alias") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(n)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("count(n)" -> 6)
      ))
    }

    test("count(*) in return without alias") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(*)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("count(*)" -> 6)
      ))
    }

    test("simple count(prop)") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(n.name) AS nonNullNames RETURN nonNullNames")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nonNullNames" -> 3)
      ))
    }

    test("simple count(node)") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(n) AS nodes RETURN nodes")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nodes" -> 6)
      ))
    }

    test("count after expand") {
      val graph = initGraph("CREATE ({name: 'foo'})-[:A]->(:B), ({name: 'bar'}), (), ()-[:A]->(:B), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n)-->(b:B) WITH count(b) AS nodes RETURN nodes")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("nodes" -> 2)
      ))
    }

    test("count() with grouping in RETURN clause") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN n.name as name, count(*) AS amount")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "amount" -> 2),
        CypherMap("name" -> null, "amount" -> 3),
        CypherMap("name" -> "baz", "amount" -> 1)
      ))
    }

    test("count() with grouping in WITH clause") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH n.name as name, count(*) AS amount RETURN name, amount")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "amount" -> 2),
        CypherMap("name" -> null, "amount" -> 3),
        CypherMap("name" -> "baz", "amount" -> 1)
      ))
    }

    test("count() with grouping on multiple keys") {
      val graph = initGraph("CREATE ({name: 'foo', age: 42}), ({name: 'foo', age: 42}), ({name: 'foo', age: 23}), (), (), ({name: 'baz', age: 23})")

      val result = graph
        .cypher("MATCH (n) WITH n.name AS name, n.age AS age, count(*) AS amount RETURN name, age, amount")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "age" -> 23, "amount" -> 1),
        CypherMap("name" -> "foo", "age" -> 42, "amount" -> 2),
        CypherMap("name" -> "baz", "age" -> 23, "amount" -> 1),
        CypherMap("name" -> null, "age" -> null, "amount" -> 2)
      ))
    }

    it("counts distinct with grouping") {
      val graph = initGraph(
        """
          |CREATE (a:Start{id: 1})
          |CREATE (a)-[:REL]->({val: "foo"})
          |CREATE (a)-[:REL]->({val: "foo"})
        """.stripMargin)

      val result = graph.cypher(
        """
          |MATCH (a:Start)-->(b)
          |
          |RETURN a.id,
          |       count(distinct b.val) as val
        """.stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1, "val" -> 1)
      ))
    }
  }

  describe("MIN") {

    test("min(prop) in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    test("min(prop) in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    test("min(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    test("min(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    test("min(prop) with single null value in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("MIN(n.val)" -> 23L)
      ))
    }

    ignore("min(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("min(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }

  describe("MAX") {

    test("max(prop) in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 84L)
      ))
    }

    test("max(prop) in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 84L)
      ))
    }

    test("max(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 42L)
      ))
    }

    test("max(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 42L)
      ))
    }

    test("max(prop) with single null value in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("MAX(n.val)" -> 42L)
      ))
    }

    ignore("simple max(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("simple max(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }


  describe("SUM") {

    test("sum(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 12)
      ))
    }

    test("sum(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 12)
      ))
    }

    test("sum(prop) with floats in WITH") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 10.5)
      ))
    }

    test("sum(prop) with floats in RETURN") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 10.5)
      ))
    }

    test("sum(prop) with floats in RETURN without alias") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val)")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("SUM(n.val)" -> 10.5)
      ))
    }

    test("simple sum(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 65.0)
      ))
    }

    test("simple sum(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> 65.0)
      ))
    }

    ignore("simple sum(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("simple sum(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }

  describe("COLLECT") {

    test("collect(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) WITH COLLECT(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq(2, 4, 6))
      ))
    }

    test("collect(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq(2, 4, 6))
      ))
    }


    test("simple collect(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH COLLECT(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq(23.0, 42.0))
      ))
    }

    test("simple collect(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq(23.0, 42.0))
      ))
    }


    test("simple collect(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH Collect(n.val) AS res RETURN res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq.empty)
      ))
    }

    test("simple collect(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("res" -> Seq.empty)
      ))
    }

    it("collects distinct lists with grouping") {
      val graph = initGraph(
        """
          |CREATE (a:Start{id: 1})
          |CREATE (a)-[:REL]->({val: "foo"})
          |CREATE (a)-[:REL]->({val: "foo"})
        """.stripMargin)

      val result = graph.cypher(
        """
          |MATCH (a:Start)-->(b)
          |
          |RETURN a.id,
          |       collect(distinct b.val) as val
        """.stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1, "val" -> CypherList("foo"))
      ))
    }
  }

  it("collects non-nullable strings that are not present on every match") {
    val graph = initGraph(
      """
        |CREATE (a:Person{id: 1, name:'Anna'})
        |CREATE (b:Person{id: 2, name:'Bob'})
        |CREATE (p1:Purchase{id: 3})
        |CREATE (a)-[:BOUGHT]->(p1)
      """.stripMargin)

    val recommendations = graph.cypher(
      """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
         |(friend)-[:IS]->(customer:Customer),
         |(customer)-[:BOUGHT]->(product:Product)
         |RETURN person.name AS for, collect(DISTINCT product.title) AS recommendations""".stripMargin)
  }

  describe("Combinations") {

    test("multiple aggregates in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |WITH
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col
          |RETURN avg, cnt, min, max, sum, col""".stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap("avg" -> 49, "cnt" -> 3, "min" -> 23L, "max" -> 84L, "sum" -> 149, "col" -> Seq(23, 42, 84))
      ))
    }

    test("multiple aggregates in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |RETURN
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col""".stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap("avg" -> 49, "cnt" -> 3, "min" -> 23L, "max" -> 84L, "sum" -> 149, "col" -> Seq(23, 42, 84))
      ))
    }

    it("computes multiple aggregates with grouping in RETURN clause") {
      val graph = initGraph("CREATE ({key: 'a', val: 42}),({key: 'a',val: 23}),({key: 'b', val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |RETURN
          | n.key AS key,
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) as col""".stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap(
          "key" -> "a", "avg" -> 32, "cnt" -> 2, "min" -> 23L, "max" -> 42L, "sum" -> 65, "col" -> Seq(23, 42)),
        CypherMap("key" -> "b", "avg" -> 84, "cnt" -> 1, "min" -> 84, "max" -> 84, "sum" -> 84, "col" -> Seq(84))
      ))
    }

    it("computes multiple aggregates with grouping in WITH clause") {
      val graph = initGraph("CREATE ({key: 'a', val: 42}),({key: 'a',val: 23}),({key: 'b', val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |WITH
          | n.key AS key,
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) as col
          |RETURN key, avg, cnt, min, max, sum, col""".stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap(
          "key" -> "a", "avg" -> 32, "cnt" -> 2, "min" -> 23L, "max" -> 42L, "sum" -> 65, "col" -> Seq(23, 42)),
        CypherMap("key" -> "b", "avg" -> 84, "cnt" -> 1, "min" -> 84, "max" -> 84, "sum" -> 84, "col" -> Seq(84))
      ))
    }
  }
}
