package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.types.CTInteger
import org.opencypher.spark.api.value._

import scala.collection.Bag

class BoundedVarExpandAcceptanceTest extends SparkCypherTestSuite {

  test("bounded to single relationship") {

    // Given
    val given = TestGraph("""(:Node {val: "source"})-->(:Node {val: "mid1"})-->(:Node {val: "end"})""")

    // When
    val result = given.cypher("MATCH (n:Node)-[r*0..1]->(m:Node) RETURN m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("m.val" -> "source"),
      CypherMap("m.val" -> "mid1"),
      CypherMap("m.val" -> "mid1"),
      CypherMap("m.val" -> "end"),
      CypherMap("m.val" -> "end")
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("bounded with lower bound") {

    // Given
    val given = TestGraph("""(:Node {val: "source"})-->(:Node {val: "mid1"})-->(:Node {val: "end"})""")

    // When
    val result = given.cypher("MATCH (t:Node)-[r*2..3]->(y:Node) RETURN y.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("y.val" -> "end")
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("var expand with default lower and loop") {
    // Given
    val given = TestGraph("""(a:Node {v: "a"})-->(:Node {v: "b"})-->(:Node {v: "c"})-->(a)""")

    // When
    val result = given.cypher("MATCH (a:Node)-[r*..6]->(b:Node) RETURN b.v")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("b.v" -> "a"),
      CypherMap("b.v" -> "a"),
      CypherMap("b.v" -> "a"),
      CypherMap("b.v" -> "b"),
      CypherMap("b.v" -> "b"),
      CypherMap("b.v" -> "b"),
      CypherMap("b.v" -> "c"),
      CypherMap("b.v" -> "c"),
      CypherMap("b.v" -> "c")
    ))

    // And
    result.graph shouldMatch given.graph
  }

  test("var expand return list of rel ids") {
    // Given
    val given = TestGraph("""(a:Node {v: "a"})-->(:Node {v: "b"})-->(:Node {v: "c"})-->(a)""")

    // When
    val result = given.cypher("MATCH (a:Node)-[r*..6]->(b:Node) RETURN r")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("r" -> CypherList(Seq(0))),
      CypherMap("r" -> CypherList(Seq(0, 1))),
      CypherMap("r" -> CypherList(Seq(0, 1, 2))),
      CypherMap("r" -> CypherList(Seq(1))),
      CypherMap("r" -> CypherList(Seq(1, 2))),
      CypherMap("r" -> CypherList(Seq(1, 2, 0))),
      CypherMap("r" -> CypherList(Seq(2))),
      CypherMap("r" -> CypherList(Seq(2, 0))),
      CypherMap("r" -> CypherList(Seq(2, 0, 1)))
    ))

    // And
    result.graph shouldMatch given.graph
  }
}
