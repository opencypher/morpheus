/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.io.neo4j

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{Neo4jServerFixture, OpenCypherDataFixture}

class Neo4jGraphLoaderTest extends CAPSTestSuite with Neo4jServerFixture with OpenCypherDataFixture {

  test("import a graph from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig)

    graph.schema should equal(schema)
    graph.nodes("n").toDF().count() shouldBe nbrNodes
    graph.relationships("r").toDF().count() shouldBe nbrRels
  }

  test("import only some nodes from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (f:Film) RETURN f", "UNWIND [] AS i RETURN i")

    graph.schema should equal(Schema.empty.withNodePropertyKeys("Film")("title" -> CTString))
    graph.nodes("n").toDF().count() shouldBe 5
    graph.relationships("r").toDF().count() shouldBe 0
  }

  test("import only some rels (and their endnodes) from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (s)-[:ACTED_IN]->(t) WITH collect(s) AS sources, collect(t) AS targets WITH sources + targets AS nodes UNWIND nodes AS n RETURN DISTINCT n", "MATCH ()-[a:ACTED_IN]->() RETURN a")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys("Person", "Actor")("name" -> CTString, "birthyear" -> CTInteger)
      .withNodePropertyKeys("Film")("title" -> CTString)
      .withRelationshipPropertyKeys("ACTED_IN")("charactername" -> CTString)
    )
    graph.nodes("n").toDF().count() shouldBe 12
    graph.relationships("r").toDF().count() shouldBe 8
  }
}
