package org.opencypher.caps.test.support.testgraph

import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.DebugOutputSupport

import scala.collection.Bag
import scala.collection.immutable.HashedBagConfiguration

class CypherCreateParserTest extends BaseTestSuite with DebugOutputSupport {

  implicit val n: HashedBagConfiguration[Node] = Bag.configuration.compact[Node]
  implicit val r: HashedBagConfiguration[Relationship] = Bag.configuration.compact[Relationship]

  test("parse single node create statement") {
    val graph =CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
      """.stripMargin)

    graph.nodes should equal(Seq(
      Node("a", 1, Set("Person"), Map("name" -> "Alice"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in single create statement") {
    val graph =CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node("a", 1, Set("Person"), Map("name" -> "Alice")),
      Node("b", 2, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }

  test("parse multiple nodes in separate create statements") {
    val graph =CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node("a", 1, Set("Person"), Map("name" -> "Alice")),
      Node("b", 2, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships should be(Seq.empty)
  }


  test("parse multiple nodes connected by relationship") {
    val graph =CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 42}]->(b:Person {name: "Bob"})
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node("a", 1, Set("Person"), Map("name" -> "Alice")),
      Node("b", 2, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      Relationship("  UNNAMED35", 3, 1, 2, "KNOWS", Map("since" -> 42))
    ))
  }

  test("parse multiple nodes and relationship in separate create statements") {
    val graph =CypherCreateParser(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (a)-[:KNOWS {since: 42}]->(b)
      """.stripMargin)

    graph.nodes.toBag should equal(Bag(
      Node("a", 1, Set("Person"), Map("name" -> "Alice")),
      Node("b", 2, Set("Person"), Map("name" -> "Bob"))
    ))

    graph.relationships.toBag should be(Bag(
      Relationship("  UNNAMED78", 3, 1, 2, "KNOWS", Map("since" -> 42))
    ))
  }

}
