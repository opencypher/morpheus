package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.value.{CypherInteger, CypherString}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.{GraphMatchingTestSupport, TestSession, TestSuiteImpl}

class ExpressionAcceptanceTest extends TestSuiteImpl with GraphMatchingTestSupport with TestSession.Fixture {

  test("less than") {
    // Given
    val inputGraph = """(:Node {val: 4L})-->(:Node {val: 5L})"""

    // When
    val result = inputGraph.toGraph.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val < m.val RETURN n.val")

    // Then
    result.records.toMaps should equal(Set(
      Map("n.val" -> CypherInteger(4))
    ))
    // And
    result.graph shouldMatch inputGraph
  }

  test("less than or equal") {
    // Given
    val inputGraph = """(:Node {id: 1L, val: 4L})-->(:Node {id: 2L, val: 5L})-->(:Node {id: 3L, val: 5L})"""

    // When
    val result = inputGraph.toGraph.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val <= m.val RETURN n.id, n.val")

    // Then
    result.records.toMaps should equal(Set(
      Map("n.id" -> CypherInteger(1), "n.val" -> CypherInteger(4)),
      Map("n.id" -> CypherInteger(2), "n.val" -> CypherInteger(5))
    ))
    // And
    result.graph shouldMatch inputGraph
  }

  test("subtraction") {
    // Given
    val inputGraph = """(:Node {val: 4L})-->(:Node {val: 5L})"""

    // When
    val result = inputGraph.toGraph.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val - n.val AS res")

    // Then
    result.records.toMaps should equal(Set(
      Map("res" -> CypherInteger(1))
    ))
    // And
    result.graph shouldMatch inputGraph
  }

  test("property expression") {
    val theGraph = """(:Person {name: "Mats"})-->(:Person {name: "Martin"})"""

    val graph = theGraph.toGraph

    val result = graph.cypher("MATCH (p:Person) RETURN p.name")

    result.records.toMaps should equal(Set(
      Map("p.name" -> CypherString("Mats")),
      Map("p.name" -> CypherString("Martin"))
    ))
    result.graph shouldMatch theGraph
  }

  test("property expression with relationship") {
    val theGraph = """(:Person {name: "Mats"})-[:KNOWS {since: 2017l}]->(:Person {name: "Martin"})"""

    val graph = theGraph.toGraph

    val result = graph.cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")

    result.records.toMaps should equal(Set(
      Map("r.since" -> CypherInteger(2017))
    ))
    result.graph shouldMatch theGraph
  }
}

