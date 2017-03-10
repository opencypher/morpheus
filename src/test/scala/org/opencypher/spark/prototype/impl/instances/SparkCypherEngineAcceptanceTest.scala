package org.opencypher.spark.prototype.impl.instances

import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.instances.spark.cypher._
import org.opencypher.spark.prototype.impl.load.SparkGraphSpace
import org.opencypher.spark.prototype.impl.syntax.cypher._
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkCypherEngineAcceptanceTest extends StdTestSuite with TestSession.Fixture {

  val schema = Schema.empty
    .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
    .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
    .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
    .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)
  val space = SparkGraphSpace.fromNeo4j(schema, "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n", "MATCH ()-[r:ATTENDED]->() RETURN r")

  test("label scan and project") {
    val resultView = space.base.cypher("MATCH (a:Event) RETURN a.link")

    resultView.show()
  }

}
