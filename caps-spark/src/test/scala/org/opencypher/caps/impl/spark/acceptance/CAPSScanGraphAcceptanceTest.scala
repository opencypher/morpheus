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
import org.opencypher.caps.api.value.CAPSMap
import org.opencypher.caps.test.support.creation.caps.{CAPSGraphFactory, CAPSScanGraphFactory}

import scala.collection.Bag

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSGraphFactory = CAPSScanGraphFactory

  describe("convenient") {

    it("matches an undirected variable-length relationship") {
      val given = initGraph(
        """
          |CREATE (a:A {prop: 'isA'})
          |CREATE (b:B {prop: 'fromA'})
          |CREATE (c:C {prop: 'toA'})
          |CREATE (d:D)
          |CREATE (a)-[:T]->(b)
          |CREATE (b)-[:T]->(c)
          |CREATE (c)-[:T]->(a)
          |CREATE (c)-[:T]->(d)
        """.stripMargin
      )

      val result = given.cypher("MATCH (a:A)-[*3..3]-(other) RETURN a.prop, other.prop")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a.prop" -> "isA", "other.prop" -> "isA"),
        CAPSMap("a.prop" -> "isA", "other.prop" -> "isA"),
        CAPSMap("a.prop" -> "isA", "other.prop" -> null)
      ))
    }

    it("matches an undirected cyclic relationship") {
      val given = initGraph(
        """
          |CREATE (a:A {prop: 'isA'})
          |CREATE (b:B)
          |CREATE (a)-[:T]->(a)
          |CREATE (b)-[:T]->(a)
        """.stripMargin
      )

      val result = given.cypher("MATCH (a:A)--(a) RETURN a.prop")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a.prop" -> "isA")
      ))
    }

    it("matches a mixed directed/undirected pattern") {
      val given = initGraph(
        """
          |CREATE (a:A {prop: 'a'})
          |CREATE (b:B {prop: 'b'})
          |CREATE (c:C {prop: 'c'})
          |CREATE (a)-[:T]->(a)
          |CREATE (a)-[:T]->(a)
          |CREATE (b)-[:T]->(a)
          |CREATE (a)-[:T]->(c)
        """.stripMargin
      )

      val result = given.cypher("MATCH (a:A)--(a)<--(other) RETURN a.prop, other.prop")

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a.prop" -> "a", "other.prop" -> "a"),
        CAPSMap("a.prop" -> "a", "other.prop" -> "a"),
        CAPSMap("a.prop" -> "a", "other.prop" -> "b"),
        CAPSMap("a.prop" -> "a", "other.prop" -> "b")
      ))
    }

    it("matches an undirected pattern with pre-bound nodes") {
      val given = initGraph(
        """
          |CREATE (a:A {prop: 'a'})
          |CREATE (b:B {prop: 'b'})
          |CREATE (b)-[:T]->(a)
          |CREATE (a)-[:T]->(b)
        """.stripMargin
      )

      val result = given.cypher(
        """
          |MATCH (a:A)
          |MATCH (b:B)
          |MATCH (a)--(b)
          |RETURN a.prop, b.prop
        """.stripMargin)

      result.records.iterator.toBag should equal(Bag(
        CAPSMap("a.prop" -> "a", "b.prop" -> "b"),
        CAPSMap("a.prop" -> "a", "b.prop" -> "b")
      ))
    }
  }
}
