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
package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.GraphSources
import org.opencypher.spark.api.io.CAPSRelationshipTable
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{CAPSNeo4jServerFixture, OpenCypherDataFixture}

import scala.language.reflectiveCalls

class CAPSRecordsAcceptanceTest extends CAPSTestSuite with CAPSNeo4jServerFixture with OpenCypherDataFixture {

  private lazy val graph: RelationalCypherGraph[DataFrameTable] =
    GraphSources.cypher.neo4j(neo4jConfig).graph(entireGraphName).asCaps

  it("convert nodes to CypherMaps") {
    // When
    val result = graph.cypher("MATCH (a:Person) WHERE a.birthyear < 1930 RETURN a, a.name")

    // Then
    result.records.collect should equal(Array(
      CypherMap("a" -> CAPSNode(0, Set("Actor", "Person"), CypherMap("birthyear" -> 1910, "name" -> "Rachel Kempson")), "a.name" -> "Rachel Kempson"),
      CypherMap("a" -> CAPSNode(1, Set("Actor", "Person"), CypherMap("birthyear" -> 1908, "name" -> "Michael Redgrave")), "a.name" -> "Michael Redgrave"),
      CypherMap("a" -> CAPSNode(10, Set("Actor", "Person"), CypherMap("birthyear" -> 1873, "name" -> "Roy Redgrave")), "a.name" -> "Roy Redgrave")
    ))
  }

  it("convert rels to CypherMaps") {
    // When
    val result = graph.cypher("MATCH ()-[r:ACTED_IN]->() WHERE r.charactername ENDS WITH 'e' RETURN r")

    // Then
    result.records.collect should equal(Array(
      CypherMap("r" -> CAPSRelationship(23, 6, 18, "ACTED_IN", CypherMap("charactername" -> "Albus Dumbledore"))),
      CypherMap("r" -> CAPSRelationship(21, 2, 20, "ACTED_IN", CypherMap("charactername" -> "Guenevere"))),
      CypherMap("r" -> CAPSRelationship(26, 8, 19, "ACTED_IN", CypherMap("charactername" -> "Halle/Annie")))
    ))
  }

  case class ActedIn(rel: Long, start: Long, end: Long)

  it("convert CAPSRelTable to CypherMaps") {
    val mapping = RelationshipMapping("rel", "start", "end", Left("ACTED_IN"))
    val table = sparkSession.createDataFrame(Seq(ActedIn(1, 2, 3), ActedIn(2, 3, 4)))
    val relTable: CAPSRelationshipTable = CAPSRelationshipTable.fromMapping(mapping, table)

    relTable.collect.toBag should equal(Bag(
      CypherMap("entity" -> CAPSRelationship(1, 2, 3, "ACTED_IN")),
      CypherMap("entity" -> CAPSRelationship(2, 3, 4, "ACTED_IN"))
    ))
  }

  it("CAPSRelTable can show") {
    val mapping = RelationshipMapping("rel", "start", "end", Left("ACTED_IN"))
    val table = sparkSession.createDataFrame(Seq(ActedIn(1, 2, 3), ActedIn(2, 3, 4)))
    val relTable: CAPSRelationshipTable = CAPSRelationshipTable.fromMapping(mapping, table)

    relTable.show(capturingPrintOptions)
    assertPrinted(expectation =
      """╔═══════════════╗
        |║ entity        ║
        |╠═══════════════╣
        |║ [:`ACTED_IN`] ║
        |║ [:`ACTED_IN`] ║
        |╚═══════════════╝
        |(2 rows)
        |""".stripMargin)
  }

  case class MyRow(rel: Long, start: Long, end: Long, rel_t: String)

  it("convert CAPSRelTable to CypherMaps with dynamic relationship types") {
    val mapping = RelationshipMapping("rel", "start", "end", Right("rel_t" -> Set("FOO", "BAR")))
    val table = sparkSession.createDataFrame(Seq(MyRow(1, 2, 3, "FOO"), MyRow(2, 3, 4, "BAR"))).setNonNullable("rel_t")
    val relTable: CAPSRelationshipTable = CAPSRelationshipTable.fromMapping(mapping, table)

    relTable.collect.toBag should equal(Bag(
      CypherMap("entity" -> CAPSRelationship(1, 2, 3, "FOO")),
      CypherMap("entity" -> CAPSRelationship(2, 3, 4, "BAR"))
    ))
  }

  it("label scan and project") {
    // When
    val result = graph.cypher("MATCH (a:Person) RETURN a.name")

    // Then
    result.records.size shouldBe 15
    result.records.collect should contain(CypherMap("a.name" -> "Rachel Kempson"))
  }

  it("expand and project") {
    // When
    val result = graph.cypher("MATCH (a:Actor)-[r]->(m:Film) RETURN a.birthyear, m.title")

    // Then
    result.records.size shouldBe 8
    result.records.collect should contain(CypherMap("a.birthyear" -> 1952, "m.title" -> "Batman Begins"))
  }

  it("filter rels on property") {
    // Given
    val query = "MATCH (a:Actor)-[r:ACTED_IN]->() WHERE r.charactername = 'Guenevere' RETURN a, r"

    // When
    val result = graph.cypher(query)

    // Then
    result.records.collect.toBag should equal(Bag(
      CypherMap(
        "a" -> CAPSNode(2, Set("Actor", "Person"), CypherMap("birthyear" -> 1937, "name" -> "Vanessa Redgrave")),
        "r" -> CAPSRelationship(21, 2, 20, "ACTED_IN", CypherMap("charactername" -> "Guenevere"))
      )
    ))
  }

  it("filter nodes on property") {
    // When
    val result = graph.cypher("MATCH (p:Person) WHERE p.birthyear = 1970 RETURN p.name")

    // Then
    result.records.collect.toBag should equal(Bag(
      CypherMap("p.name" -> "Fake Bar"),
      CypherMap("p.name" -> "Fake Foo"),
      CypherMap("p.name" -> "Christopher Nolan")
    ))
  }

  it("expand and project, three properties") {
    // Given
    val query = "MATCH (a:Actor)-[:ACTED_IN]->(f:Film) RETURN a.name, f.title, a.birthyear"

    // When
    val result = graph.cypher(query)

    // Then
    result.records.size shouldBe 8
    result.records.collect should contain(CypherMap("a.name" -> "Natasha Richardson", "f.title" -> "The Parent Trap", "a.birthyear" -> 1963))
  }

  it("multiple hops of expand with different reltypes") {
    // Given
    val query = "MATCH (c:City)<-[:BORN_IN]-(a:Actor)-[r:ACTED_IN]->(f:Film) RETURN a.name, c.name, f.title"

    // When
    val records = graph.cypher(query).records

    records.asCaps.df.collect().toBag should equal(Bag(
        Row("Natasha Richardson", "London", "The Parent Trap"),
        Row("Dennis Quaid", "Houston", "The Parent Trap"),
        Row("Lindsay Lohan", "New York", "The Parent Trap"),
        Row("Vanessa Redgrave", "London", "Camelot")
      ))
  }
}
