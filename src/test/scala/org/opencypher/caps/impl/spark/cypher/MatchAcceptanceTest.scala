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

class MatchAcceptanceTest extends CAPSTestSuite {

  test("multiple match clauses") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p3:Person {name: "Eve"}),
        |(p1)-[:KNOWS]->(p2),
        |(p2)-[:KNOWS]->(p3),
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)
        |MATCH (p1:Person)-[e1]->(p2:Person)
        |MATCH (p2)-[e2]->(p3:Person)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Eve"
      )
    ))
    result.graphs shouldBe empty
  }

  test("cyphermorphism and multiple match clauses") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p1)-[:KNOWS]->(p2),
        |(p2)-[:KNOWS]->(p1)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person)-[e2:KNOWS]->(p3:Person)
        |MATCH (p3)-[e3:KNOWS]->(p4:Person)
        |RETURN p1.name, p2.name, p3.name, p4.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> "Alice",
        "p3.name" -> "Bob",
        "p4.name" -> "Alice"
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Alice",
        "p4.name" -> "Bob"
      )
    ))
    result.graphs shouldBe empty
  }

  test("disconnected components") {
    // Given
    val given = TestGraph(
      """
        |(p1:Narcissist {name: "Alice"}),
        |(p2:Narcissist {name: "Bob"}),
        |(p1)-[:LOVES]->(p1),
        |(p2)-[:LOVES]->(p2)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (a:Narcissist), (b:Narcissist)
        |RETURN a.name AS one, b.name AS two
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("one" -> "Alice", "two" -> "Alice"),
      CypherMap("one" -> "Alice", "two" -> "Bob"),
      CypherMap("one" -> "Bob", "two" -> "Bob"),
      CypherMap("one" -> "Bob", "two" -> "Alice")
    ))
  }

  test("joined components") {
    // Given
    val given = TestGraph(
      """
        |(p1:Narcissist {name: "Alice"}),
        |(p2:Narcissist {name: "Bob"}),
        |(p1)-[:LOVES]->(p1),
        |(p2)-[:LOVES]->(p2)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (a:Narcissist), (b:Narcissist) WHERE a.name = b.name
        |RETURN a.name AS one, b.name AS two
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("one" -> "Alice", "two" -> "Alice"),
      CypherMap("one" -> "Bob", "two" -> "Bob")
    ))

    result.explain.print

    // TODO: Move to plan based testing
    result.explain.toString should include("ValueJoin(predicates = [a.name :: STRING = b.name :: STRING]")
  }
}
