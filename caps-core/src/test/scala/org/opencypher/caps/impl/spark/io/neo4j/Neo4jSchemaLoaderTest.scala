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
import org.opencypher.caps.test.fixture.Neo4jServerFixture

class Neo4jSchemaLoaderTest extends CAPSTestSuite with Neo4jServerFixture {

  val emptyQ = "WITH 1 AS a LIMIT 0 RETURN *"

  test("read empty") {
    val graph = Neo4jGraphLoader.loadSchema(neo4jConfig, emptyQ, emptyQ)

    graph.schema should equal(Schema.empty)
  }

  test("read nodes") {
    val graph = Neo4jGraphLoader.loadSchema(neo4jConfig, "MATCH (n) RETURN n", emptyQ)

    graph.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable, "salary" -> CTInteger.nullable, "team" -> CTString.nullable)
        .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger, "team" -> CTString.nullable)
        .withNodePropertyKeys("Driver")("name" -> CTString.nullable, "fast" -> CTBoolean.nullable)
        .withNodePropertyKeys("")("pi" -> CTFloat.nullable)
    )
  }

  test("read relationships") {
    val graph = Neo4jGraphLoader.loadSchema(neo4jConfig, emptyQ, "MATCH ()-[r]->() RETURN r")

    graph.schema should equal(
      Schema.empty
        .withRelationshipPropertyKeys("EMPTY")()
        .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "because" -> CTString.nullable)
    )
  }

  test("load full graph") {
    val graph = Neo4jGraphLoader.loadSchema(neo4jConfig, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")

    graph.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable, "salary" -> CTInteger.nullable, "team" -> CTString.nullable)
        .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger, "team" -> CTString.nullable)
        .withNodePropertyKeys("Driver")("name" -> CTString.nullable, "fast" -> CTBoolean.nullable)
        .withNodePropertyKeys("")("pi" -> CTFloat.nullable)
        .withRelationshipPropertyKeys("EMPTY")()
        .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "because" -> CTString.nullable)
    )
  }

  override def dataFixture =
    """CREATE (me:Person {name: 'me'}),
      |       (you:Person {name: 'you', age: 9}),
      |       (:Person:Employee {name: 'x', salary: 123}),
      |       (:Person:Employee {name: 'y', salary: 456, team: 'cypher'}),
      |       (:Person:Driver {name: 'mdm scott'}),
      |       (:Driver {fast: true}),
      |       (empty),
      |       (almostEmpty {pi: 3.14})
      |CREATE (me)-[:KNOWS {since: 1969}]->(you)
      |CREATE (me)-[:KNOWS {since: 1969, because: 'reason'}]->(you)
      |CREATE (empty)-[:EMPTY]->(empty)
    """.stripMargin
}
