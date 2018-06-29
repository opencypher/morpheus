/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.io.neo4j

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.neo4j.io.Neo4jServerFixture
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.CAPSTestSuite

class Neo4jSchemaLoaderTest extends CAPSTestSuite with Neo4jServerFixture {

  val emptyQ = "WITH 1 AS a LIMIT 0 RETURN *"

  test("read empty") {
    val schema = Neo4jGraphLoader.loadSchema(neo4j, emptyQ, emptyQ)

    schema should equal(CAPSSchema.empty)
  }

  test("read nodes") {
    val schema = Neo4jGraphLoader.loadSchema(neo4j, "MATCH (n) RETURN n", emptyQ)

    schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("Person", "Employee")(
          "name" -> CTString,
          "salary" -> CTInteger,
          "team" -> CTString.nullable)
        .withNodePropertyKeys("Person", "Driver")("name" -> CTString)
        .withNodePropertyKeys("Driver")("fast" -> CTBoolean)
        .withNodePropertyKeys(Set.empty[String], Map("pi" -> CTFloat.nullable))
        .asCaps
    )
  }

  test("read relationships") {
    val schema = Neo4jGraphLoader.loadSchema(neo4j, emptyQ, "MATCH ()-[r]->() RETURN r")

    schema should equal(
      Schema.empty
        .withRelationshipPropertyKeys("EMPTY")()
        .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "because" -> CTString.nullable)
        .asCaps
    )
  }

  test("load full graph") {
    val schema = Neo4jGraphLoader.loadSchema(neo4j, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")

    schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger.nullable)
        .withNodePropertyKeys("Person", "Employee")(
          "name" -> CTString,
          "salary" -> CTInteger,
          "team" -> CTString.nullable)
        .withNodePropertyKeys("Person", "Driver")("name" -> CTString)
        .withNodePropertyKeys("Driver")("fast" -> CTBoolean)
        .withNodePropertyKeys(Set.empty[String], Map("pi" -> CTFloat.nullable))
        .withRelationshipPropertyKeys("EMPTY")()
        .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "because" -> CTString.nullable)
        .asCaps
    )
  }

  override def dataFixture: String =
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
