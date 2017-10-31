/*
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
package org.opencypher.caps.impl.spark.io.neo4j

import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.{Neo4jServerFixture, OpenCypherDataFixture}

class Neo4jGraphLoaderTest extends CAPSTestSuite with Neo4jServerFixture with OpenCypherDataFixture {

  test("import a graph from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig)

    graph.nodes("n").toDF().count() shouldBe nbrNodes
    graph.relationships("r").toDF().count() shouldBe nbrRels
    graph.schema should equal(schema)
  }

  test("import only some nodes from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (f:Film) RETURN f", "UNWIND [] AS i RETURN i")

    graph.nodes("n").toDF().count() shouldBe 5
    graph.relationships("r").toDF().count() shouldBe 0
    graph.schema should equal(Schema.empty.withNodePropertyKeys("Film")("title" -> CTString))
  }

  test("import only some rels (and their endnodes) from Neo4j") {
    val graph = Neo4jGraphLoader.fromNeo4j(neo4jConfig, "MATCH (s)-[:ACTED_IN]->(t) WITH collect(s) AS sources, collect(t) AS targets WITH sources + targets AS nodes UNWIND nodes AS n RETURN DISTINCT n", "MATCH ()-[a:ACTED_IN]->() RETURN a")

    graph.nodes("n").toDF().count() shouldBe 12
    graph.relationships("r").toDF().count() shouldBe 8
    graph.schema should equal(Schema.empty
      .withRelationshipPropertyKeys("ACTED_IN")("charactername" -> CTString)
      .withNodePropertyKeys("Person")("name" -> CTString, "birthyear" -> CTInteger)
      .withNodePropertyKeys("Actor")("name" -> CTString, "birthyear" -> CTInteger)
      .withNodePropertyKeys("Film")("title" -> CTString)
    )
  }
}
