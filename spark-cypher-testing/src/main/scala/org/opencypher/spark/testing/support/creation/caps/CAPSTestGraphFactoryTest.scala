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
package org.opencypher.spark.testing.support.creation.caps

import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.support.GraphMatchingTestSupport

abstract class CAPSTestGraphFactoryTest extends CAPSTestSuite with GraphMatchingTestSupport  {
  def factory: CAPSTestGraphFactory

  val createQuery: String =
    """
      |CREATE (max:Person:Astronaut {name: "Max"})
      |CREATE (martin:Person:Martian {name: "Martin"})
      |CREATE (swedish:Language {title: "Swedish"})
      |CREATE (german:Language {title: "German"})
      |CREATE (orbital:Language {title: "Orbital"})
      |CREATE (max)-[:SPEAKS]->(swedish)
      |CREATE (max)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(orbital)
    """.stripMargin

  val personTable: CAPSNodeTable = CAPSNodeTable.fromMapping(NodeMapping
    .on("ID")
    .withImpliedLabel("Person")
    .withOptionalLabel("Astronaut" -> "IS_ASTRONAUT")
    .withOptionalLabel("Martian" -> "IS_MARTIAN")
    .withPropertyKey("name" -> "NAME"), caps.sparkSession.createDataFrame(
    Seq(
      (0L, true, false, "Max"),
      (1L, false, true, "Martin"))
  ).toDF("ID", "IS_ASTRONAUT", "IS_MARTIAN", "NAME"))

  val languageTable: CAPSNodeTable = CAPSNodeTable.fromMapping(NodeMapping
    .on("ID")
    .withImpliedLabel("Language")
    .withPropertyKey("title" -> "TITLE"), caps.sparkSession.createDataFrame(
    Seq(
      (2L, "Swedish"),
      (3L, "German"),
      (4L, "Orbital"))
  ).toDF("ID", "TITLE"))

  val knowsScan: CAPSRelationshipTable = CAPSRelationshipTable.fromMapping(RelationshipMapping
    .on("ID")
    .from("SRC").to("DST").relType("KNOWS"), caps.sparkSession.createDataFrame(
    Seq(
      (0L, 5L, 2L),
      (0L, 6L, 3L),
      (1L, 7L, 3L),
      (1L, 8L, 4L))
  ).toDF("SRC", "ID", "DST"))

  test("testSchema") {
    val propertyGraph = CreateGraphFactory(createQuery)
    factory(propertyGraph).schema should equal(Schema.empty
      .withNodePropertyKeys("Person", "Astronaut")("name" -> CTString)
      .withNodePropertyKeys("Person", "Martian")("name" -> CTString)
      .withNodePropertyKeys("Language")("title" -> CTString)
      .withRelationshipType("SPEAKS")
      .asCaps)
  }

  test("testAsScanGraph") {
    val propertyGraph = CreateGraphFactory(createQuery)
    factory(propertyGraph).asCaps shouldMatch caps.graphs.create(personTable, languageTable, knowsScan)
  }
}

class SingleTableGraphFactoryTest extends CAPSTestGraphFactoryTest {
  override def factory: CAPSTestGraphFactory = SingleTableGraphFactory
}

class CAPSScanGraphFactoryTest extends CAPSTestGraphFactoryTest {
  override def factory: CAPSTestGraphFactory = CAPSScanGraphFactory
}
