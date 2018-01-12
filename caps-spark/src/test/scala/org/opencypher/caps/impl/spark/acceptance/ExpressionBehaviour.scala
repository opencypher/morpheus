/*
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
package org.opencypher.caps.impl.spark.acceptance

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.value.CypherMap

import scala.collection.Bag

trait ExpressionBehaviour {
  self: AcceptanceTest =>

  def expressionBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("equality between properties") {
      // Given
      val given = initGraph(
        """
          |CREATE (:A {val: 1})-[:REL]->(:B {p: 2})
          |CREATE (:A {val: 2})-[:REL]->(:B {p: 1})
          |CREATE (:A {val: 100})-[:REL]->(:B {p: 100})
          |CREATE (:A {val: 1})-[:REL]->(:B)
          |CREATE (:A)-[:REL]->(:B {p: 2})
          |CREATE (:A)-[:REL]->(:B)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a:A)-->(b:B) RETURN a.val = b.p AS eq")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("eq" -> false),
        CypherMap("eq" -> false),
        CypherMap("eq" -> true),
        CypherMap("eq" -> null),
        CypherMap("eq" -> null),
        CypherMap("eq" -> null)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("less than") {

      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN n.val < m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val < m.val" -> true),
        CypherMap("n.val < m.val" -> false),
        CypherMap("n.val < m.val" -> false),
        CypherMap("n.val < m.val" -> null)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("less than or equal") {
      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN n.val <= m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val <= m.val" -> true),
        CypherMap("n.val <= m.val" -> true),
        CypherMap("n.val <= m.val" -> false),
        CypherMap("n.val <= m.val" -> null)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("greater than") {
      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN n.val > m.val AS gt")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("gt" -> false),
        CypherMap("gt" -> false),
        CypherMap("gt" -> true),
        CypherMap("gt" -> null)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("greater than or equal") {
      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN n.val >= m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val >= m.val" -> false),
        CypherMap("n.val >= m.val" -> true),
        CypherMap("n.val >= m.val" -> true),
        CypherMap("n.val >= m.val" -> null)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("addition") {
      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5, other: 3})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN m.other + m.val + n.val AS res")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 12),
        CypherMap("res" -> null)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("subtraction with name") {
      // Given
      val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5, other: 3})-[:REL]->()")

      // When
      val result = given.cypher("MATCH (n)-->(m) RETURN m.val - n.val - m.other AS res")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("res" -> -2),
        CypherMap("res" -> null)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("subtraction without name") {
      // Given
      val given = initGraph("CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val - n.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("m.val - n.val" -> 1)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("multiplication with integer") {
      // Given
      val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val: 2})-[:REL]->(:Node {val: 3})")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val * m.val" -> 18),
        CypherMap("n.val * m.val" -> 6)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("multiplication with float") {
      // Given
      val given = initGraph("CREATE (:Node {val: 4.5D})-[:REL]->(:Node {val: 2.5D})")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val * m.val" -> 11.25)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("multiplication with integer and float") {
      // Given
      val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val2: 2.5D})")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val2")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val * m.val2" -> 22.5)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("division with no remainder") {
      // Given
      val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val: 3})-[:REL]->(:Node {val: 2})")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val / m.val" -> 3),
        CypherMap("n.val / m.val" -> 1)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("division integer and float and null") {
      // Given
      val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val2: 4.5D})-[:REL]->(:Node)")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val2")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val / m.val2" -> 2.0),
        CypherMap("n.val / m.val2" -> null)
      ))

      // And
      result.graphs shouldBe empty
    }

    ignore("equality") {
      // Given
      val given = initGraph(
        """
          |CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})
          |CREATE (:Node {val: 4})-[:REL]->(:Node {val: 4})
          |CREATE (:Node)-[:REL]->(:Node {val: 5})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val = n.val AS res")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("res" -> false),
        CypherMap("res" -> true),
        CypherMap("res" -> null)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("property expression") {
      // Given
      val given = initGraph("CREATE (:Person {name: 'Mats'})-[:REL]->(:Person {name: 'Martin'})")

      // When
      val result = given.cypher("MATCH (p:Person) RETURN p.name")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("p.name" -> "Mats"),
        CypherMap("p.name" -> "Martin")
      ))

      result.graphs shouldBe empty
    }

    test("property expression with relationship") {
      // Given
      val given = initGraph("CREATE (:Person {name: 'Mats'})-[:KNOWS {since: 2017}]->(:Person {name: 'Martin'})")

      // When
      val result = given.cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("r.since" -> 2017)
      ))

      result.graphs shouldBe empty
    }
  }
}
