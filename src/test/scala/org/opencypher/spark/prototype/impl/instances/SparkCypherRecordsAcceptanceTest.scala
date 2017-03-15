package org.opencypher.spark.prototype.impl.instances

import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.prototype.api.expr.Var
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.instances.spark.cypher._
import org.opencypher.spark.prototype.impl.syntax.cypher._
import org.opencypher.spark.prototype.api.spark.SparkGraphSpace
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkCypherRecordsAcceptanceTest extends StdTestSuite with TestSession.Fixture {

  test("label scan and project") {
    val records = initSmallSpace().base.cypher("MATCH (a:User) RETURN a.text").records

    records.data.count() shouldBe 1806
    records.data.head().getString(0) shouldBe a[String]
    records.header.slots.size shouldBe 1
    records.header.slots.head.content.cypherType shouldBe CTString.nullable
    records.header.slots.head.content.key should equal(Var("a.text"))
  }

  test("expand and project") {
    val records = initSmallSpace().base.cypher("MATCH (a:User)-[r]->(m:Meetup) RETURN a.country, m.id").records

    records.data.count() shouldBe 4832
    records.header.slots.size shouldBe 2
    records.header.slots(0).content.cypherType shouldBe CTString.nullable
    records.header.slots(0).content.key should equal(Var("a.country"))
    records.header.slots(1).content.cypherType shouldBe CTInteger.nullable
    records.header.slots(1).content.key should equal(Var("m.id"))
  }

  test("expand and project on full graph") {
    val records = fullSpace.base.cypher("MATCH (g:Graph)-[r]->(e:Event) RETURN g.key, e.title").records

    records.data.count() shouldBe 25
    records.header.slots.size shouldBe 2
    records.header.slots(0).content.cypherType shouldBe CTString.nullable
    records.header.slots(0).content.key should equal(Var("a.country"))
    records.header.slots(1).content.cypherType shouldBe CTInteger.nullable
    records.header.slots(1).content.key should equal(Var("m.id"))
  }

  test("handle properties with same key and different type between labels") {
    val space = initSmallSpace(Schema.empty
      .withNodeKeys("Channel")("id" -> CTString.nullable)
      .withNodeKeys("GitHub")("id" -> CTInteger.nullable), "MATCH (n) RETURN n", "RETURN 1 LIMIT 0")

    val channels = space.base.cypher("MATCH (c:Channel) RETURN c.id").records

    channels.data.count() shouldBe 78
    channels.header.slots(0).content.cypherType shouldBe CTString.nullable

    val githubs = space.base.cypher("MATCH (g:GitHub) RETURN g.id").records

    githubs.data.count() shouldBe 365
    githubs.header.slots(0).content.cypherType shouldBe CTInteger.nullable
  }

  private val smallSchema = Schema.empty
    .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
    .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
    .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
    .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)

  private def initSmallSpace(schema: Schema = smallSchema,
                             nodeQ: String = "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n",
                             relQ: String = "MATCH ()-[r:ATTENDED]->() RETURN r") = {
    SparkGraphSpace.fromNeo4j(nodeQ, relQ, Some(schema))
  }

  private lazy val fullSpace = SparkGraphSpace.fromNeo4j("MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")
}
