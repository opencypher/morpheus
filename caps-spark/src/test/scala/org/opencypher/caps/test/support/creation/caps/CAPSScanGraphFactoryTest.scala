package org.opencypher.caps.test.support.creation.caps

import org.opencypher.caps.api.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.CTString
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.support.GraphMatchingTestSupport
import org.opencypher.caps.test.support.creation.propertygraph.CAPSPropertyGraphFactory

class CAPSScanGraphFactoryTest extends CAPSTestSuite with GraphMatchingTestSupport {

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

  val personScan: NodeScan = NodeScan.on("p" -> "ID") {
    _.build
        .withImpliedLabel("Person")
        .withOptionalLabel("Astronaut" -> "IS_ASTRONAUT")
        .withOptionalLabel("Martian" -> "IS_MARTIAN")
        .withPropertyKey("name" -> "NAME")
  }.from(CAPSRecords.create(
    Seq("ID", "IS_ASTRONAUT", "IS_MARTIAN", "NAME"),
    Seq(
      (0L, true, false, "Max"),
      (1L, false, true, "Martin"))
  ))

  val languageScan: NodeScan = NodeScan.on("l" -> "ID") {
    _.build
        .withImpliedLabel("Language")
        .withPropertyKey("title" -> "TITLE")
  }.from(CAPSRecords.create(
    Seq("ID", "TITLE"),
    Seq(
      (2L, "Swedish"),
      (3L, "German"),
      (4L, "Orbital"))
  ))

  val knowsScan: RelationshipScan = RelationshipScan.on("k" -> "ID") {
    _.from("SRC").to("DST").relType("KNOWS")
        .build
  }.from(CAPSRecords.create(
    Seq("SRC", "ID", "DST"),
    Seq(
      (0L, 5L, 2L),
      (0L, 6L, 3L),
      (1L, 7L, 3L),
      (1L, 8L, 4L))
  ))

  test("testSchema") {
    val propertyGraph = CAPSPropertyGraphFactory(createQuery)
    CAPSScanGraphFactory(propertyGraph).schema should equal(Schema.empty
        .withNodePropertyKeys("Person", "Astronaut")("name" -> CTString)
        .withNodePropertyKeys("Person", "Martian")("name" -> CTString)
        .withNodePropertyKeys("Language")("title" -> CTString)
        .withRelationshipType("SPEAKS"))
  }

  test("testAsScanGraph") {
    val propertyGraph = CAPSPropertyGraphFactory(createQuery)
    CAPSScanGraphFactory(propertyGraph) shouldMatch CAPSGraph.create(personScan, languageScan, knowsScan)
  }
}
