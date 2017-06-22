package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.value.CypherMap
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.{GraphMatchingTestSupport, TestSuiteImpl}

class PredicateAcceptanceTest extends TestSuiteImpl with GraphMatchingTestSupport {

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
}
