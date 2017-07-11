package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

class WithAcceptanceTest extends SparkCypherTestSuite {

  test("rebinding of dropped variables") {
    // Given
    val given = TestGraph("""(:Node {val: 1l}), (:Node {val: 2L})""")

    // When
    val result = given.cypher(
      """MATCH (n:Node)
        |WITH n.val AS foo
        |WITH foo + 2 AS bar
        |WITH bar + 2 AS foo
        |RETURN foo
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("foo" -> 5),
      CypherMap("foo" -> 6)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("projecting constants") {
    // Given
    val given = TestGraph("""(), ()""")

    // When
    val result = given.cypher(
      """MATCH ()
        |WITH 3 AS foo
        |WITH foo + 2 AS bar
        |RETURN bar
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("bar" -> 5),
      CypherMap("bar" -> 5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

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

  test("projecting property expression with filter") {

    // Given
    val given = TestGraph("""(:Node {val: 3L}), (:Node {val: 4L}), (:Node {val: 5L})""")

    // When
    val result = given.cypher("MATCH (n:Node) WITH n.val AS n_val WHERE n_val <= 4 RETURN n_val")

    // Then
    result.records.toMaps should equal(Set(
      CypherMap("n_val" -> 3),
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
