package org.opencypher.spark.prototype.impl.load

import org.apache.spark.sql.types._
import org.opencypher.spark.prototype.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.prototype.api.expr.Var
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkGraphSpace}
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkGraphSpaceTest extends StdTestSuite with TestSession.Fixture {

  implicit class RichGraph(val graph: SparkCypherGraph) {
    def nodes() = graph.nodes(Var("n"))
    def rels() = graph.relationships(Var("r"))
  }

  test("import nodes from neo") {
    val schema = Schema.empty
      .withNodeKeys("Tweet")("id" -> CTInteger, "text" -> CTString.nullable, "created" -> CTString.nullable)
    val space = SparkGraphSpace.fromNeo4j("MATCH (n:Tweet) RETURN n LIMIT 100", "RETURN 1 LIMIT 0", schema)
    val df = space.base.nodes().details.toDF()

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false,
      BooleanType -> false,
      LongType -> false,
      StringType -> true,
      StringType -> true
    ))
  }

  test("import relationships from neo") {
    val schema = Schema.empty
      .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    val space = SparkGraphSpace.fromNeo4j(
      "RETURN 1 LIMIT 0",
      "MATCH ()-[r:ATTENDED]->() RETURN r LIMIT 100", schema)
    val df = space.base.rels().details.toDF

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false,
      IntegerType -> false,
      LongType -> false,
      StringType -> true
    ))
  }

  test("import a graph from neo") {
    val schema = Schema.empty
      .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
      .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
      .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
      .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
      .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)
    val space = SparkGraphSpace.fromNeo4j(
      "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n",
      "MATCH ()-[r:ATTENDED]->() RETURN r", schema)
    val rels = space.base.rels().details.toDF
    val nodes = space.base.nodes().details.toDF

    rels.count() shouldBe 4832
    nodes.count() shouldBe 2901
  }

  test("read schema from loaded neo graph") {
    val schema = SparkGraphSpace.loadSchema("MATCH (a) RETURN a", "MATCH ()-[r]->() RETURN r").schema

    schema.labels.size shouldBe 32
    schema.relationshipTypes.size shouldBe 14
    schema.nodeKeys("User").size shouldBe 37  // number of unique prop keys for all nodes of that label
    schema.relationshipKeys("ATTENDED").size shouldBe 5
  }
}
