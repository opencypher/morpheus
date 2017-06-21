package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.value.CypherMap
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.{GraphMatchingTestSupport, TestSuiteImpl}
import org.opencypher.spark.{GraphMatchingTestSupport, TestSession, TestSuiteImpl}

class ExpressionAcceptanceTest extends TestSuiteImpl with GraphMatchingTestSupport {

  test("less than") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val < m.val RETURN n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n.val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("less than or equal") {
    // Given
    val given = TestGraph("""(:Node {id: 1L, val: 4L})-->(:Node {id: 2L, val: 5L})-->(:Node {id: 3L, val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val <= m.val RETURN n.id, n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n.id" -> 1, "n.val" -> 4),
      CypherMap("n.id" -> 2, "n.val" -> 5)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("greater than") {
    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val:5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val > m.val RETURN n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n.val" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("greater than or equal") {
    // Given
    val given = TestGraph("""(:Node {id: 1L, val: 4L})-->(:Node {id: 2L, val: 5L})-->(:Node {id: 3L, val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val >= m.val RETURN n.id, n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n.id" -> 2, "n.val" -> 5),
      CypherMap("n.id" -> 3, "n.val" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("addition") {
    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L, other: 3L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.other + m.val + n.val AS res")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("res" -> 12)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("subtraction with name") {
    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L, other: 3L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val - n.val - m.other AS res")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("res" -> -2)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  ignore("equality") {
    // Given
    val given = TestGraph(
      """(:Node {val: 4L})-->(:Node {val: 5L}),
        |(:Node {val: 4L})-->(:Node {val: 4L}),
        |(:Node)-->(:Node {val: 5L})
      """.stripMargin)

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val = n.val AS res")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("res" -> false),
      CypherMap("res" -> true),
      CypherMap("res" -> null)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("subtraction without name") {
    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val - n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("m.val - n.val" -> 1)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("property expression") {
    // Given
    val given = TestGraph("""(:Person {name: "Mats"})-->(:Person {name: "Martin"})""")

    // When
    val result = given.cypher("MATCH (p:Person) RETURN p.name")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("p.name" -> "Mats"),
      CypherMap("p.name" -> "Martin")
    ))

    result.graph shouldMatch given.graph
  }

  test("property expression with relationship") {
    // Given
    val given = TestGraph("""(:Person {name: "Mats"})-[:KNOWS {since: 2017l}]->(:Person {name: "Martin"})""")

    // When
    val result = given.cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("r.since" -> 2017)
    ))

    result.graph shouldMatch given.graph
  }

}

