package org.opencypher.spark.prototype.impl.load

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkGraphSpaceTest extends StdTestSuite with TestSession.Fixture {

  test("import nodes from neo") {
    val schema = Schema.empty
      .withNodeKeys("Tweet")("id" -> CTInteger, "text" -> CTString.nullable, "created" -> CTInteger.nullable)
    val space = SparkGraphSpace.fromNeo4j(schema, "MATCH (n:Tweet) RETURN n LIMIT 100", "RETURN 1 LIMIT 0")
    val df = space.base.nodes.records.toDF

    df.count() shouldBe 100
    df.schema.fields.toSet should equal(Set(
      StructField("n", LongType, nullable = false),
      StructField("label_Tweet", BooleanType, nullable = false),
      StructField("prop_id", LongType, nullable = false),
      StructField("prop_text", StringType, nullable = true),
      StructField("prop_created", LongType, nullable = true)
    ))
  }

  test("import relationships from neo") {
    val schema = Schema.empty
      .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    val space = SparkGraphSpace.fromNeo4j(schema, "RETURN 1 LIMIT 0", "MATCH ()-[r:ATTENDED]->() RETURN r LIMIT 100")
    val df = space.base.relationships.records.toDF

    df.count() shouldBe 100
    df.schema.fields.toSet should equal(Set(
      StructField("r", LongType, nullable = false),
      StructField("type", IntegerType, nullable = false),
      StructField("prop_guests", LongType, nullable = false),
      StructField("prop_comments", StringType, nullable = true)
    ))
  }

  test("import a graph from neo") {
    val schema = Schema.empty
      .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
      .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
      .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
      .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
      .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)
    val space = SparkGraphSpace.fromNeo4j(schema, "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n", "MATCH ()-[r:ATTENDED]->() RETURN r")
    val rels = space.base.relationships.records.toDF
    val nodes = space.base.nodes.records.toDF

    rels.count() shouldBe 4832
    nodes.count() shouldBe 2901
  }

  test("import a graph and query it") {
    val schema = Schema.empty
      .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
      .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
      .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
      .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
      .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)
    val space = SparkGraphSpace.fromNeo4j(schema, "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n", "MATCH ()-[r:ATTENDED]->() RETURN r")

    val resultView = space.base.cypher("MATCH (a:Event) RETURN a.link")

    resultView.show()
  }
}
