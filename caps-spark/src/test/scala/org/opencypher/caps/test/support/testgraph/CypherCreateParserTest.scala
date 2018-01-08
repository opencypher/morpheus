package org.opencypher.caps.test.support.testgraph

import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.DebugOutputSupport

import scala.collection.Bag
import scala.collection.immutable.HashedBagConfiguration

class CypherCreateParserTest extends BaseTestSuite with DebugOutputSupport {

  implicit val n: HashedBagConfiguration[Node] = Bag.configuration.compact[Node]
  implicit val r: HashedBagConfiguration[Relationship] = Bag.configuration.compact[Relationship]

  test("parse single node create statement") {
    val graph = CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
      """.stripMargin)

    graph.nodes should equal(Seq(
      Node(0, Set("Person"), Map("name" -> "Alice"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in single create statement") {
    val graph = CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set("Person"), Map("name" -> "Alice")),
      Node(1, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in separate create statements") {
    val graph = CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set("Person"), Map("name" -> "Alice")),
      Node(1, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes connected by relationship") {
    val graph = CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 42}]->(b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set("Person"), Map("name" -> "Alice")),
      Node(1, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      Relationship(2, 0, 1, "KNOWS", Map("since" -> 42))
    ))
  }

  test("parse multiple nodes and relationship in separate create statements") {
    val graph = CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (a)-[:KNOWS {since: 42}]->(b)
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set("Person"), Map("name" -> "Alice")),
      Node(1, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      Relationship(2, 0, 1, "KNOWS", Map("since" -> 42))
    ))
  }

  test("simple unwind") {
    val graph = CypherCreateParser(
      """
        |UNWIND [1,2,3] as i
        |CREATE (a {val: i})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set(), Map("val" -> 1)),
      Node(1, Set(), Map("val" -> 2)),
      Node(2, Set(), Map("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("stacked unwind") {
    val graph = CypherCreateParser(
      """
        |UNWIND [1,2,3] AS i
        |UNWIND [4] AS j
        |CREATE (a {val1: i, val2: j})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set(), Map("val1" -> 1, "val2" -> 4)),
      Node(1, Set(), Map("val1" -> 2, "val2" -> 4)),
      Node(2, Set(), Map("val1" -> 3, "val2" -> 4))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("unwind with variable reference") {
    val graph = CypherCreateParser(
      """
        |UNWIND [[1,2,3]] AS i
        |UNWIND i AS j
        |CREATE (a {val: j})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node(0, Set(), Map("val" -> 1)),
      Node(1, Set(), Map("val" -> 2)),
      Node(2, Set(), Map("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }

  test("unwind with parameter reference") {
    val graph = CypherCreateParser(
      """
        |UNWIND $i AS j
        |CREATE (a {val: j})
      """.stripMargin, Map("i" -> List(1, 2, 3)))

    graph.nodes.toBag should equal(Bag(
      Node(0, Set(), Map("val" -> 1)),
      Node(1, Set(), Map("val" -> 2)),
      Node(2, Set(), Map("val" -> 3))
    ))

    graph.relationships.toBag shouldBe empty
  }
}
