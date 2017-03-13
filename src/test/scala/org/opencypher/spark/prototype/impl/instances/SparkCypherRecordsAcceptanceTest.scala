package org.opencypher.spark.prototype.impl.instances

import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.prototype.api.expr.Var
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.instances.spark.cypher._
import org.opencypher.spark.prototype.impl.syntax.cypher._
import org.opencypher.spark.prototype.api.spark.SparkGraphSpace
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkCypherRecordsAcceptanceTest extends StdTestSuite with TestSession.Fixture {

  val schema = Schema.empty
    .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
    .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
    .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
    .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)

  val space = SparkGraphSpace.fromNeo4j(schema, "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n", "MATCH ()-[r:ATTENDED]->() RETURN r")

  test("label scan and project") {
    val records = space.base.cypher("MATCH (a:User) RETURN a.text").records

    records.data.count() shouldBe 1806
    records.data.head().getString(0) shouldBe a[String]
    records.header.slots.size shouldBe 1
    records.header.slots.head.content.cypherType shouldBe CTString.nullable
    records.header.slots.head.content.key should equal(Var("a.text"))
  }

  test("expand and project") {
    val resultView = space.base.cypher("MATCH (a:User)-->(m:Meetup) RETURN a.country, m.id")

    resultView.records.data.count() shouldBe 1000
    resultView.records.header.slots.size shouldBe 2
    resultView.records.header.slots(0).content.cypherType shouldBe CTString.nullable
    resultView.records.header.slots(0).content.key should equal(Var("a.country"))
    resultView.records.header.slots(1).content.cypherType shouldBe CTInteger.nullable
    resultView.records.header.slots(1).content.key should equal(Var("m.id"))
  }

}
