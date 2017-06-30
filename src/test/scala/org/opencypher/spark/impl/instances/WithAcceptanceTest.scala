package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.value.CypherMap
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.{GraphMatchingTestSupport, TestSuiteImpl}

class WithAcceptanceTest extends TestSuiteImpl with GraphMatchingTestSupport {

  test("projecting variables in scope") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n, m RETURN n.val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n.val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting property expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val AS n_val RETURN n_val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n_val" -> 4)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting addition expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val + m.val AS sum_n_m_val RETURN sum_n_m_val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("sum_n_m_val" -> 9)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("aliasing variables") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val + m.val AS sum WITH sum AS sum2 RETURN sum2")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("sum2" -> 9)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting mixed expression") {

    // Given
    val given = TestGraph("""(:Node {val: 4L})-->(:Node {val: 5L})-->(:Node)""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val AS n_val, n.val + m.val AS sum_n_m_val RETURN sum_n_m_val, n_val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("sum_n_m_val" -> 9, "n_val" -> 4),
      CypherMap("sum_n_m_val" -> null, "n_val" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }
}
