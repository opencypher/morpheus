package org.opencypher.caps.impl.instances

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.value.CypherMap

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
    result.graph shouldMatch given.graph
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
    result.graph shouldMatch given.graph
  }
}
