package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

import scala.collection.Bag

class OptionalMatchAcceptanceTest extends SparkCypherTestSuite {

  test("optional match") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p1)-[:KNOWS]->(p2),
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)
        |OPTIONAL MATCH (p1)-[e1]->(p2)
        |RETURN p1.name, p2.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> null
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob"
      )
    ))

    result.graph shouldMatch given.graph
  }
}
