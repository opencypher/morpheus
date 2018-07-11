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

import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class ReturnBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("RETURN") {
    it("returns only the returned fields") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      val result = g.cypher("MATCH (a:A) WITH a, a.name AS foo RETURN a")

      result.records.collect.toBag should equal(Bag(
        CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap("name" -> "me"))),
        CypherMap("a" -> CAPSNode(1L, Set("A"), CypherMap.empty))
      ))
    }

    it("returns only returned fields with tricky alias") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      val result = g.cypher("MATCH (a:A) WITH a, a AS foo RETURN a")

      result.records.collect.toBag should equal(Bag(
        CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap("name" -> "me"))),
        CypherMap("a" -> CAPSNode(1L, Set("A"), CypherMap.empty))
      ))
    }

    it("return only returned fields with trickier aliasing") {
      val g = initGraph("CREATE (:A {name: 'me'}), (:A)")

      // we need to somehow track lineage of aliased entities
      // perhaps copy all child expressions in RecordHeader
      val result = g.cypher("MATCH (a:A) WITH a, a AS foo RETURN foo AS b")

      result.records.collect.toBag should equal(Bag(
        CypherMap("b" -> CAPSNode(0L, Set("A"), CypherMap("name" -> "me"))),
        CypherMap("b" -> CAPSNode(1L, Set("A"), CypherMap.empty))
      ))
    }

    it("returns only returned fields without dependencies") {
      val g = initGraph("CREATE (:A)")

      val result = g.cypher("MATCH (a:A), (b) RETURN a")

      result.records.collect.toBag should equal(Bag(
        CypherMap("a" -> CAPSNode(0L, Set("A"), CypherMap.empty))
      ))
    }

    it("can run a single return query") {
      val given = initGraph("CREATE ()")

      val result = given.cypher("RETURN 1").asCaps

      result.getRecords shouldMatch CypherMap("1" -> 1)
    }

    it("can run single return query with several columns") {
      val given = initGraph("CREATE (), ()")

      val result = given.cypher("RETURN 1 AS foo, '' AS str").asCaps

      result.getRecords shouldMatch CypherMap("foo" -> 1, "str" -> "")
    }

    it("returns full node") {
      val given = initGraph("CREATE ({foo:'bar'}),()")

      val result = given.cypher("MATCH (n) RETURN n")

      result.records.toMaps should equal(Bag(
        CypherMap("n" -> 0, "n.foo" -> "bar"),
        CypherMap("n" -> 1, "n.foo" -> null))
      )
    }

    it("returns full rel") {
      val given = initGraph("CREATE ()-[:Rel {foo:'bar'}]->()-[:Rel]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN r")

      result.records.collect.toBag should equal(Bag(
        CypherMap("r" -> CAPSRelationship(2, 0, 1, "Rel", CypherMap("foo" -> "bar"))),
        CypherMap("r" -> CAPSRelationship(4, 1, 3, "Rel"))
      ))
    }

    it("returns relationship property from relationship without specific type") {
      val given = initGraph("CREATE ()-[:Rel {foo:'bar'}]->()-[:Rel]->()")

      val result = given.cypher("MATCH ()-[r]->() RETURN r.foo")

      result.records.toMaps should equal(Bag(
        CypherMap("r.foo" -> "bar"),
        CypherMap("r.foo" -> null)
      ))
    }

    it("should be able to project expression with multiple references") {
      val graph = initGraph("""CREATE ({val: 0})""")

      val query =
        """
          |MATCH (a)
          |WITH a, a.val as foo
          |WITH a, foo as bar
          |RETURN a.val
        """.stripMargin


      graph.cypher(query).records.collect.toBag should equal(Bag(
        CypherMap("a.val" -> 0)
      ))
    }
  }

  describe("DISTINCT") {
    it("can return distinct properties") {
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
        CypherMap("name" -> "bar"),
        CypherMap("name" -> "foo"),
        CypherMap("name" -> "baz")
      ))
    }

    it("can return distinct properties for combinations") {
      val given = initGraph(
        """CREATE ({p1:'a', p2: 'a', p3: '1'})
          |CREATE ({p1:'a', p2: 'a', p3: '2'})
          |CREATE ({p1:'a', p2: 'b', p3: '3'})
          |CREATE ({p1:'b', p2: 'a', p3: '4'})
          |CREATE ({p1:'b', p2: 'b', p3: '5'})
        """.stripMargin)

      val result = given.cypher("MATCH (n) RETURN DISTINCT n.p1 as p1, n.p2 as p2")

      result.records.toMaps should equal(Bag(
        CypherMap("p2" -> "a", "p1" -> "a"),
        CypherMap("p2" -> "a", "p1" -> "b"),
        CypherMap("p2" -> "b", "p1" -> "a"),
        CypherMap("p2" -> "b", "p1" -> "b")
      ))
    }
  }

  describe("ORDER BY") {
    it("can order with default direction") {
      val given = initGraph("""CREATE (:Node {val: 4}), (:Node {val: 3}), (:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val AS val ORDER BY val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
    }

    it("can order ascending") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val ASC")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
    }

    it("can order descending") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val DESC")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 42L),
        CypherMap("val" -> 4L),
        CypherMap("val" -> 3L)
      ))
    }
  }
  describe("SKIP") {
    it("can skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val SKIP 2").asCaps

      // Then
      result.getRecords.df.count() should equal(1)
    }

    it("can order with skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 4L),
        CypherMap("val" -> 42L)
      ))
    }

    it("can order with (arithmetic) skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 + 1")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 42L)
      ))
    }
  }

  describe("limit") {
    it("can evaluate limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val LIMIT 1").asCaps

      // Then
      result.getRecords.df.count() should equal(1)
    }

    it("can evaluate limit with parameter value") {
      val graph = initGraph("CREATE (a:A),(b:B),(c:C)")

      val res = graph.cypher(
        """
          |MATCH (a)
          |WITH a
          |LIMIT $limit
          |RETURN a""".stripMargin, Map("limit" -> CypherValue(1)))

      res.records.size
    }


    it("can order with limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 3L)
      ))
    }

    it("can order with (arithmetic) limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val LIMIT 1 + 1")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 3L),
        CypherMap("val" -> 4L)
      ))
    }

    it("can order with skip and limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) RETURN a.val as val ORDER BY val SKIP 1 LIMIT 1")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("val" -> 4L)
      ))
    }
  }
}
