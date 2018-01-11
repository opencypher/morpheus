package org.opencypher.caps.impl.spark.acceptanceFunSpecMixin

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.value.CypherMap

import scala.collection.Bag

trait ExpandIntoBehaviour {
  self: AcceptanceTest =>
  
  def expandIntoBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("test expand into for dangling edge") {
      // Given
      val given = initGraph(
        """
          |CREATE (p1:Person {name: "Alice"})
          |CREATE (p2:Person {name: "Bob"})
          |CREATE (p3:Person {name: "Eve"})
          |CREATE (p4:Person {name: "Carl"})
          |CREATE (p5:Person {name: "Richard"})
          |CREATE (p1)-[:KNOWS]->(p2)
          |CREATE (p2)-[:KNOWS]->(p3)
          |CREATE (p1)-[:KNOWS]->(p3)
          |CREATE (p3)-[:KNOWS]->(p4)
          |CREATE (p3)-[:KNOWS]->(p5)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
          |(p2)-[e2:KNOWS]->(p3:Person),
          |(p1)-[e3:KNOWS]->(p3),
          |(p3)-[e4:KNOWS]->(p4)
          |RETURN p1.name, p2.name, p3.name, p4.name
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap(
          "p1.name" -> "Alice",
          "p2.name" -> "Bob",
          "p3.name" -> "Eve",
          "p4.name" -> "Carl"
        ),
        CypherMap(
          "p1.name" -> "Alice",
          "p2.name" -> "Bob",
          "p3.name" -> "Eve",
          "p4.name" -> "Richard"
        )
      ))

      result.graphs shouldBe empty
    }

    test("test expand into for triangle") {
      // Given
      val given = initGraph(
        """
          |CREATE (p1:Person {name: "Alice"})
          |CREATE (p2:Person {name: "Bob"})
          |CREATE (p3:Person {name: "Eve"})
          |CREATE (p1)-[:KNOWS]->(p2)
          |CREATE (p2)-[:KNOWS]->(p3)
          |CREATE (p1)-[:KNOWS]->(p3)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
          |(p2)-[e2:KNOWS]->(p3:Person),
          |(p1)-[e3:KNOWS]->(p3)
          |RETURN p1.name, p2.name, p3.name
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap(
          "p1.name" -> "Alice",
          "p2.name" -> "Bob",
          "p3.name" -> "Eve"
        )
      ))

      result.graphs shouldBe empty
    }

    test("Expand into after var expand") {
      // Given
      val given = initGraph(
        """
          |CREATE (p1:Person {name: "Alice"})
          |CREATE (p2:Person {name: "Bob"})
          |CREATE (comment:Comment)
          |CREATE (post1:Post {content: "asdf"})
          |CREATE (post2:Post {content: "foobar"})
          |CREATE (p1)-[:KNOWS]->(p2)
          |CREATE (p2)<-[:HASCREATOR]-(comment)
          |CREATE (comment)-[:REPLYOF]->(post1)-[:REPLYOF]->(post2)
          |CREATE (post2)-[:HASCREATOR]->(p1)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
          |      (p2)<-[e2:HASCREATOR]-(comment:Comment),
          |      (comment)-[e3:REPLYOF*1..10]->(post:Post),
          |      (p1)<-[:HASCREATOR]-(post)
          |WHERE p1.name = "Alice"
          |RETURN p1.name, p2.name, post.content
        """.stripMargin
      )

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap(
          "p1.name" -> "Alice",
          "p2.name" -> "Bob",
          "post.content" -> "foobar"
        )
      ))
    }
  }
}
