package org.opencypher.spark.api.io.sql

import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import GraphDdlOps._

class GraphDdlOpsTest extends BaseTestSuite {

  describe("convert GraphType to OKAPI schema") {
    it("converts a graph type with single element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          | Person ( name STRING, age INTEGER ),
          | Book   ( title STRING ) ,
          | READS  ( rating FLOAT ) ,
          | (Person),
          | (Book),
          | (Person)-[READS]->(Book)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
        .withNodePropertyKeys("Book")("title" -> CTString)
        .withRelationshipPropertyKeys("READS")("rating" -> CTFloat)
        .withSchemaPatterns(SchemaPattern("Person", "READS", "Book")))
    }

    it("converts a graph type with multiple element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A (x STRING),
          |  B (y STRING),
          |  R (y STRING),
          |  (A),
          |  (A, B),
          |  (A)-[R]->(A),
          |  (A, B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

    it("converts a graph type with element type inheritance") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A           (x STRING),
          |  B EXTENDS A (y STRING),
          |  R (y STRING),
          |  (A),
          |  (B),
          |  (A)-[R]->(A),
          |  (B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

  }
}
