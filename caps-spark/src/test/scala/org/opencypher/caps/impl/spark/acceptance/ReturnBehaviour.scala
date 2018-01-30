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

import org.opencypher.caps.api.value.{CAPSMap, CAPSNode, Properties}
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.immutable.Bag

trait ReturnBehaviour { this: AcceptanceTest =>

  def returnBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("return only the returned fields") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      val result = g.cypher("MATCH (a:A) WITH a, a.name AS foo RETURN a")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties("name" -> "me"))),
        CAPSMap("a" -> CAPSNode(1L, Seq("A"), Properties.empty))
      ))
    }

    test("return only returned fields with tricky alias") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      val result = g.cypher("MATCH (a:A) WITH a, a AS foo RETURN a")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties("name" -> "me"))),
        CAPSMap("a" -> CAPSNode(1L, Seq("A"), Properties.empty))
      ))
    }

    ignore("return only returned fields with trickier aliasing") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      PrintLogicalPlan.set()

      // we need to somehow track lineage of aliased entities
      // perhaps copy all child expressions in RecordHeader
      val result = g.cypher("MATCH (a:A) WITH a, a AS foo RETURN foo AS b")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties("name" -> "me"))),
        CAPSMap("a" -> CAPSNode(1L, Seq("A"), Properties.empty))
      ))
    }

    test("return only returned fields without dependencies") {
      val g = initGraph("CREATE (:A)")

      val result = g.cypher("MATCH (a:A), (b) RETURN a")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a" -> CAPSNode(0L, Seq("A"), Properties.empty))
      ))
    }

    test("single return query") {
      val given = initGraph("CREATE ()")

      val result  = given.cypher("RETURN 1").asCaps

      result.records shouldMatch CAPSMap("1" -> 1)
    }

    test("single return query with several columns") {
      val given = initGraph("CREATE (), ()")

      val result  = given.cypher("RETURN 1 AS foo, '' AS str").asCaps

      result.records shouldMatch CAPSMap("foo" -> 1, "str" -> "")
    }

    test("return compact node") {
      val given = initGraph("CREATE (:Person {foo:'bar'}),()")

      val result = given.cypher("MATCH (n) RETURN n").asCaps

      result.records.compact.toMaps should equal(Bag(
        CAPSMap("n" -> 0),
        CAPSMap("n" -> 1))
      )
    }

    test("return full node") {
      val given = initGraph("CREATE ({foo:'bar'}),()")

      val result = given.cypher("MATCH (n) RETURN n")

      result.records.toMaps should equal(Bag(
        CAPSMap("n" -> 0, "n.foo" -> "bar"),
        CAPSMap("n" -> 1, "n.foo" -> null))
      )
    }

    test("return compact rel") {
      val given = initGraph("CREATE ()-[:Rel {foo:'bar'}]->()-[:Rel]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN r").asCaps

      result.records.compact.toMaps should equal(Bag(
        CAPSMap("r" -> 2),
        CAPSMap("r" -> 4)
      ))
    }

    test("return full rel") {
      val given = initGraph("CREATE ()-[:Rel {foo:'bar'}]->()-[:Rel]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN r")

      result.records.toMaps should equal(Bag(
        CAPSMap("r" -> 2, "source(r)" -> 0, "target(r)" -> 1, "type(r)" -> "Rel", "r.foo" -> "bar"),
        CAPSMap("r" -> 4, "source(r)" -> 1, "target(r)" -> 3, "type(r)" -> "Rel", "r.foo" -> null)
      ))
    }

    test("return relationship property from relationship without specific type") {
      val given = initGraph("CREATE ()-[:Rel {foo:'bar'}]->()-[:Rel]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN r.foo")

      result.records.toMaps should equal(Bag(
        CAPSMap("r.foo" -> "bar"),
        CAPSMap("r.foo" -> null)
      ))
    }

    test("return distinct properties") {
      val given = initGraph(
        """CREATE ({name:'bar'})
          |CREATE ({name:'bar'})
          |CREATE ({name:'baz'})
          |CREATE ({name:'baz'})
          |CREATE ({name:'bar'})
          |CREATE ({name:'foo'})
        """.stripMargin)

      val result = given.cypher("MATCH (n) RETURN DISTINCT n.name AS name")

      result.records.toMaps should equal(Bag(
        CAPSMap("name" -> "bar"),
        CAPSMap("name" -> "foo"),
        CAPSMap("name" -> "baz")
      ))
    }

    test("return distinct properties for combinations") {
      val given = initGraph(
        """CREATE ({p1:'a', p2: 'a', p3: '1'})
          |CREATE ({p1:'a', p2: 'a', p3: '2'})
          |CREATE ({p1:'a', p2: 'b', p3: '3'})
          |CREATE ({p1:'b', p2: 'a', p3: '4'})
          |CREATE ({p1:'b', p2: 'b', p3: '5'})
        """.stripMargin)

      val result = given.cypher("MATCH (n) RETURN DISTINCT n.p1 as p1, n.p2 as p2")

      result.records.toMaps should equal(Bag(
        CAPSMap("p1" -> "a", "p2" -> "a"),
        CAPSMap("p1" -> "a", "p2" -> "b"),
        CAPSMap("p1" -> "b", "p2" -> "a"),
        CAPSMap("p1" -> "b", "p2" -> "b")
      ))
    }

    test("order by") {
      val given = initGraph("""CREATE (:Node {val: 4}), (:Node {val: 3}), (:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val AS val ORDER BY val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 3L),
        CAPSMap("val" -> 4L),
        CAPSMap("val" -> 42L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("order by asc") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val ASC")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 3L),
        CAPSMap("val" -> 4L),
        CAPSMap("val" -> 42L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("order by desc") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val DESC")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 42L),
        CAPSMap("val" -> 4L),
        CAPSMap("val" -> 3L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val SKIP 2").asCaps

      // Then
      result.records.toDF().count() should equal(1)

      // And
      result.graphs shouldBe empty
    }

    test("order by with skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 4L),
        CAPSMap("val" -> 42L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with (arithmetic) skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 + 1")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 42L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val LIMIT 1").asCaps

      // Then
      result.records.toDF().count() should equal(1)

      // And
      result.graphs shouldBe empty
    }

    test("order by with limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 3L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with (arithmetic) limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1 + 1")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 3L),
        CAPSMap("val" -> 4L)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with skip and limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 LIMIT 1")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("val" -> 4L)
      ))

      // And
      result.graphs shouldBe empty
    }
  }
}
