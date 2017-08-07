package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

import scala.collection.Bag

class ExpandIntoAcceptanceTest extends SparkCypherTestSuite {

  test("test expand into") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {firstName: "Alice", lastName: "Foo"}),
        |(p2:Person {firstName: "Bob", lastName: "Foo"}),
        |(p3:Person {firstName: "Eve", lastName: "Foo"}),
        |(p1)-[:KNOWS]->(p2),
        |(p2)-[:KNOWS]->(p3),
        |(p1)-[:KNOWS]->(p3),
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
        |(p2)-[e2:KNOWS]->(p3:Person),
        |(p1)-[e3:KNOWS]->(p3)
        |RETURN p1.firstName, p1.lastName, p2.firstName, p2.lastName, p3.firstName, p3.lastName
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.firstName" -> "Alice",
        "p1.lastName" -> "Foo",
        "p2.firstName" -> "Bob",
        "p2.lastName" -> "Foo",
        "p3.firstName" -> "Eve",
        "p3.lastName" -> "Foo"
      )
    ))

    result.graph shouldMatch given.graph
  }
}
