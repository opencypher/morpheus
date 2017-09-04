/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.io.neo4j

import java.net.URI

import org.apache.spark.sql.types._
import org.neo4j.driver.v1.Config
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.types._
import org.opencypher.caps.demo.Configuration.{Neo4jAddress, Neo4jPassword, Neo4jUser}
import org.opencypher.caps.{CAPSTestSuite, Neo4jTestSession}

class Neo4jGraphLoaderTest extends CAPSTestSuite with Neo4jTestSession.Fixture {

  override val neo4jConfig = new EncryptedNeo4jConfig(URI.create(Neo4jAddress.get()),
    Neo4jUser.get(),
    Option(Neo4jPassword.get()),
    Config.EncryptionLevel.REQUIRED)

  implicit class RichGraph(val graph: CAPSGraph) {
    def nodes() = graph.nodes("n")
    def rels() = graph.relationships("r")
  }

  test("import nodes from neo") {
    val schema = Schema.empty
      .withNodePropertyKeys("Tweet")("id" -> CTInteger, "text" -> CTString.nullable, "created" -> CTString.nullable)
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (n:Tweet) RETURN n LIMIT 100", "RETURN 1 LIMIT 0", schema)
    val df = graph.nodes().toDF()

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false
    ))
  }

  test("import nodes from neo with details") {
    val schema = Schema.empty
      .withNodePropertyKeys("Tweet")("id" -> CTInteger, "text" -> CTString.nullable, "created" -> CTString.nullable)
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (n:Tweet) RETURN n LIMIT 100", "RETURN 1 LIMIT 0", schema)
    val df = graph.nodes().details.toDF()

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false,
      BooleanType -> false,
      LongType -> true,
      StringType -> true,
      StringType -> true
    ))
  }

  test("import relationships from neo") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    val space = Neo4jGraphLoader.fromNeo4j(
      neo4jConfig,
      "RETURN 1 LIMIT 0",
      "MATCH ()-[r:ATTENDED]->() RETURN r LIMIT 100", schema)
    val df = space.rels().toDF()

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false
    ))
  }

  test("import relationships from neo with details") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    val graph = Neo4jGraphLoader.fromNeo4j(
      neo4jConfig,
      "RETURN 1 LIMIT 0",
      "MATCH ()-[r:ATTENDED]->() RETURN r LIMIT 100", schema)
    val df = graph.rels().details.toDF()

    df.count() shouldBe 100
    df.schema.fields.map(f => f.dataType -> f.nullable).toSet should equal(Set(
      LongType -> false,
      IntegerType -> false,
      LongType -> true,
      StringType -> true
    ))
  }

  test("import a graph from neo") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
      .withNodePropertyKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
      .withNodePropertyKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
      .withNodePropertyKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
      .withNodePropertyKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)
    val graph = Neo4jGraphLoader.fromNeo4j(
      neo4jConfig,
      "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n",
      "MATCH ()-[r:ATTENDED]->() RETURN r", schema)
    val rels = graph.rels().toDF()
    val nodes = graph.nodes().toDF()

    rels.count() shouldBe 4832
    nodes.count() shouldBe 2901
  }

  test("read schema from loaded neo graph") {
    val schema = Neo4jGraphLoader.loadSchema(neo4jConfig, "MATCH (a) RETURN a", "MATCH ()-[r]->() RETURN r").schema

    schema.labels.size shouldBe 32
    schema.relationshipTypes.size shouldBe 14
    schema.nodeKeys("User").size shouldBe 37  // number of unique prop keys for all nodes of that label
    schema.relationshipKeys("ATTENDED").size shouldBe 5
  }
}
