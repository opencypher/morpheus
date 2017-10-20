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
    val given = TestGraph("[]")

    val result  = given.cypher("RETURN 1")

    result.records shouldMatch CypherMap("1" -> 1)
  }

  test("single return query with several columns") {
    val given = TestGraph("(), ()")

    val result  = given.cypher("RETURN 1 AS foo, '' AS str")

    result.records shouldMatch CypherMap("foo" -> 1, "str" -> "")
  }

  test("return compact node") {
    val given = TestGraph("(:Person {foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.records.compact.toMaps should equal(Bag(
      CypherMap("n" -> 0),
      CypherMap("n" -> 1))
    )
  }

  test("return full node") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.records.toMaps should equal(Bag(
      CypherMap("n" -> 0, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> "bar"),
      CypherMap("n" -> 1, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> null))
    )
  }

  test("return compact rel") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    result.records.compact.toMaps should equal(Bag(
      CypherMap("r" -> 0),
      CypherMap("r" -> 1)
    ))
  }

  test("return full rel") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()").graph

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    result.records.toMaps should equal(Bag(
      CypherMap("r" -> 0, "source(r)" -> 0, "target(r)" -> 1, "type(r)" -> DEFAULT_LABEL, "r.foo" -> "bar"),
      CypherMap("r" -> 1, "source(r)" -> 1, "target(r)" -> 2, "type(r)" -> DEFAULT_LABEL, "r.foo" -> null)
    ))
  }

  test("return relationship property from relationship without specific type") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()").graph

    val result = given.cypher("MATCH ()-[r]->() RETURN r.foo")

    result.records.toMaps should equal(Bag(
      CypherMap("r.foo" -> "bar"),
      CypherMap("r.foo" -> null)
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

  test("order by") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val AS val ORDER BY val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("order by asc") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val ASC")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("order by desc") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val DESC")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 42L),
      CypherMap("val" -> 4L),
      CypherMap("val" -> 3L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val SKIP 2")

    // Then
    result.records.toDF().count() should equal(1)

    // And
    result.graphs shouldBe empty
  }

  test("order by with skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 4L),
      CypherMap("val" -> 42L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("order by with (arithmetic) skip") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 + 1")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 42L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val LIMIT 1")

    // Then
    result.records.toDF().count() should equal(1)

    // And
    result.graphs shouldBe empty
  }

  test("order by with limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("order by with (arithmetic) limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1 + 1")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 3L),
      CypherMap("val" -> 4L)
    ))

    // And
    result.graphs shouldBe empty
  }

  test("order by with skip and limit") {
    val given = TestGraph("""(:Node {val: 4L}),(:Node {val: 3L}),(:Node  {val: 42L})""")

    val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 LIMIT 1")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("val" -> 4L)
    ))

    // And
    result.graphs shouldBe empty
  }
}
