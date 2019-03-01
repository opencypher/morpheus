/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.neo4j.io

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.harness.internal.{InProcessNeo4j, TestNeo4jBuilders}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.SchemaFromProcedure.setFlagMessage
import org.opencypher.okapi.testing.BaseTestSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class SchemaFromProcedureTest extends BaseTestSuite with BeforeAndAfter with BeforeAndAfterAll {

  private val createForNumericProperty =
    """|CREATE (:A {val1: 'String', val2: 1})
       |CREATE (:A {val1: 'String', val2: 1.2})""".stripMargin

  it("node property with numeric typed property") {
    val e = the[SchemaException] thrownBy schemaFor(createForNumericProperty)
    e.msg should (include("multiple property types") and
      include("node label combination [:A]") and
      include("val2") and
      include("Long") and
      include("Double") and
      include(setFlagMessage))
  }

  it("node property with numeric typed property: import failures omitted") {
    schemaFor(createForNumericProperty, omitImportFailures = true) should equal(
      Schema.empty
        .withNodePropertyKeys("A")("val1" -> CTString)
    )
  }

  it("input order does not affect schema") {
    val schema = schemaFor(
      """|CREATE (b1:B { type: 'B1' })
         |CREATE (b2:B { type: 'B2', size: 5 })""".stripMargin)
    schema should equal(Schema.empty
      .withNodePropertyKeys("B")("type" -> CTString, "size" -> CTInteger.nullable)
    )
  }

  it("single and multiple labels") {
    val schema = schemaFor(
      """|CREATE (:A {val1: 'String'})
         |CREATE (:B {val2: 2})
         |CREATE (:A:B {val1: 'String', val2: 2})""".stripMargin)
    schema should equal(Schema.empty
      .withNodePropertyKeys("A")("val1" -> CTString)
      .withNodePropertyKeys("B")("val2" -> CTInteger)
      .withNodePropertyKeys("A", "B")("val1" -> CTString, "val2" -> CTInteger)
    )
  }

  it("label with empty label") {
    val schema = schemaFor("CREATE ({val1: 'String'})")
    schema should equal(Schema.empty
      .withNodePropertyKeys()("val1" -> CTString)
    )
  }

  it("label without properties") {
    val schema = schemaFor("CREATE (:A)")
    schema should equal(Schema.empty
      .withNodePropertyKeys("A")()
    )
  }

  it("nullable property") {
    val schema = schemaFor("CREATE (:A {val: 1}), (:A)")
    schema should equal(Schema.empty
      .withNodePropertyKeys("A")("val" -> CTInteger.nullable)
    )
  }

  private val createForAnyProperty =
    """|CREATE (a:A)
       |CREATE (b:A)
       |CREATE (a)-[:REL {val1: 'String', val2: true}]->(b)
       |CREATE (a)-[:REL {val1: 'String', val2: 2.0}]->(b)""".stripMargin

  it("relationship with any type property") {
    val e = the[SchemaException] thrownBy schemaFor(createForAnyProperty)
    e.msg should (include("multiple property types") and
      include("relationship type [:REL]") and
      include("val2") and
      include("Boolean") and
      include("Double") and
      include(setFlagMessage))
  }

  it("relationship with any type property: import failures omitted") {
    schemaFor(createForAnyProperty, omitImportFailures = true) should equal(
      Schema.empty
        .withNodePropertyKeys("A")()
        .withRelationshipPropertyKeys("REL")("val1" -> CTString)
    )
  }

  it("relationship without properties") {
    val schema = schemaFor(
      """|CREATE (a:A)
         |CREATE (b:A)
         |CREATE (a)-[:REL]->(b)""".stripMargin)
    schema should equal(Schema.empty
      .withNodePropertyKeys("A")()
      .withRelationshipPropertyKeys("REL")()
    )
  }

  private val createForUnsupportedProperty: String =
    """|CREATE (a:A { foo: time(), bar : 42 })
       |CREATE (b:A)
       |CREATE (a)-[:REL]->(b)""".stripMargin

  it("unsupported properties") {
    val e = the[SchemaException] thrownBy schemaFor(createForUnsupportedProperty)
    e.msg should (include("unsupported property type") and
      include("Time") and
      include(setFlagMessage))
  }

  it("unsupported properties: import failures omitted") {
    schemaFor(createForUnsupportedProperty, omitImportFailures = true) should equal(
      Schema.empty
        .withNodePropertyKeys("A")("bar" -> CTInteger.nullable)
        .withRelationshipPropertyKeys("REL")()
    )
  }

  def testProperty(propertyValue: String, expectedType: CypherType): Unit = {
    schemaFor(s"CREATE (:A {p: $propertyValue})") should equal(
      Schema.empty.withNodePropertyKeys("A")("p" -> expectedType)
    )
  }

  it("is not sensitive to creation order") {
    schemaFor(
      """|CREATE (b1:B { type: 'B1' })
         |CREATE (b2:B { type: 'B2', size: 5 })""".stripMargin) should equal(Schema.empty
      .withNodePropertyKeys("B")("type" -> CTString, "size" -> CTInteger.nullable)
    )
  }

  it("supports boolean") {
    testProperty("true", CTBoolean)
  }

  it("supports boolean list") {
    testProperty("[true, false]", CTList(CTBoolean))
  }

  it("supports integer") {
    testProperty("1", CTInteger)
  }

  it("supports integer list") {
    testProperty("[1, 2]", CTList(CTInteger))
  }

  it("supports float") {
    testProperty("1.1", CTFloat)
  }

  it("supports float list") {
    testProperty("[1.2, 2.3]", CTList(CTFloat))
  }

  it("supports string") {
    testProperty("'a'", CTString)
  }

  it("supports string list") {
    testProperty("['a', 'b']", CTList(CTString))
  }

  it("supports date") {
    testProperty("date()", CTDate)
  }

  it("supports date list") {
    testProperty("[date(), date()]", CTList(CTDate))
  }

  it("supports localdatetime") {
    testProperty("localdatetime('2015-06-24T12:50:35.556')", CTLocalDateTime)
  }

  it("supports localdatetime list") {
    testProperty("[localdatetime('2015-06-24T12:50:35.556'), localdatetime('2016-06-24T12:50:35.556')]", CTList(CTLocalDateTime))
  }

  private var neo4j: InProcessNeo4j = _

  private var neo4jConfig: Neo4jConfig = _

  def graph: GraphDatabaseService = neo4j.graph()

  before {
    neo4jConfig.cypherWithNewSession("MATCH (n) DETACH DELETE n")
  }

  // TODO: Duplicate of Neo4jServerFixture
  override def beforeAll(): Unit = {
    neo4j = TestNeo4jBuilders
      .newInProcessBuilder()
      .build()
    neo4jConfig = Neo4jConfig(neo4j.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)
  }

  override def afterAll(): Unit = {
    neo4j.close()
  }

  def schemaFor(createQuery: String, omitImportFailures: Boolean = false): Schema = {
    graph.execute(createQuery).close()
    SchemaFromProcedure(neo4jConfig, omitImportFailures = omitImportFailures)
  }

}
