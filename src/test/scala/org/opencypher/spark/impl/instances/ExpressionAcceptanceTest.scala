package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

import scala.collection.Bag

class ExpressionAcceptanceTest extends SparkCypherTestSuite {

  test("less than") {

    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L})-->({val: 5L})-->({val: 2L})-->()""")

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
    result.graph shouldMatch given.graph
  }

  test("less than or equal") {
    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L})-->({val: 5L})-->({val: 2L})-->()""")

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
    result.graph shouldMatch given.graph
  }

  test("greater than") {
    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L})-->({val: 5L})-->({val: 2L})-->()""")

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
    result.graph shouldMatch given.graph
  }

  test("greater than or equal") {
    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L})-->({val: 5L})-->({val: 2L})-->()""")

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
    result.graph shouldMatch given.graph
  }

  test("addition") {
    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L, other: 3L})-->()""")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN m.other + m.val + n.val AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 12),
      CypherMap("res" -> null)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("subtraction with name") {
    // Given
    val given = TestGraph("""({val: 4L})-->({val: 5L, other: 3L})-->()""")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN m.val - n.val - m.other AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> -2),
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
    result.records.toMaps should equal(Bag(
      CypherMap("m.val - n.val" -> 1)
    ))
    // And
    result.graph shouldMatch given.graph
  }

  test("multiplication with integer") {
    // Given
    val given = TestGraph("""(:Node {val: 9L})-->(:Node {val: 2L})-->(:Node {val: 3L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val" -> 18),
      CypherMap("n.val * m.val" -> 6)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("multiplication with float") {
    // Given
    val given = TestGraph("""(:Node {val: 4.5D})-->(:Node {val: 2.5D})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val" -> 11.25)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("multiplication with integer and float") {
    // Given
    val given = TestGraph("""(:Node {val: 9L})-->(:Node {val2: 2.5D})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val2")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val2" -> 22.5)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("division with no remainder") {
    // Given
    val given = TestGraph("""(:Node {val: 9L})-->(:Node {val: 3L})-->(:Node {val: 2L})""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val / m.val" -> 3),
      CypherMap("n.val / m.val" -> 1)
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("division integer and float and null") {
    // Given
    val given = TestGraph("""(:Node {val: 9L})-->(:Node {val2: 4.5D})-->(:Node)""")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val2")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val / m.val2" -> 2.0),
      CypherMap("n.val / m.val2" -> null)
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
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> false),
      CypherMap("res" -> true),
      CypherMap("res" -> null)
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
    result.records.toMaps should equal(Bag(
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
    result.records.toMaps should equal(Bag(
      CypherMap("r.since" -> 2017)
    ))

    result.graph shouldMatch given.graph
  }
}

