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
package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.operators.Start
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{GraphConstructionFixture, RecordsVerificationFixture, TeamDataFixture}

import scala.reflect.runtime.universe

abstract class CAPSGraphTest extends CAPSTestSuite
  with GraphConstructionFixture
  with RecordsVerificationFixture
  with TeamDataFixture {

  object CAPSGraphTest {
    implicit class RecordOps(records: RelationalCypherRecords[DataFrameTable]) {
      def planStart: Start[DataFrameTable] = {
        implicit val tableTypeTag: universe.TypeTag[DataFrameTable] = caps.tableTypeTag
        implicit val context: RelationalRuntimeContext[DataFrameTable] = caps.basicRuntimeContext()
        Start.fromEmptyGraph(records)
      }
    }
  }

  it("should return only nodes with that exact label (single label)") {
    val graph = initGraph(dataFixtureWithoutArrays)
    val nodes = graph.nodes("n", CTNode("Person"), exactLabelMatch = true)
    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    verify(nodes, cols, Bag(Row(4L, true, 8L, "Donald")))
  }

  it("should return only nodes with that exact label (multiple labels)") {
    val graph = initGraph(dataFixtureWithoutArrays)
    val nodes = graph.nodes("n", CTNode("Person", "German"), exactLabelMatch = true)
    val cols = Seq(
      n,
      nHasLabelGerman,
      nHasLabelPerson,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row(2L, true, true, 1337L, "Martin"),
      Row(3L, true, true, 8L, "Max"),
      Row(0L, true, true, 42L, "Stefan")
    )
    verify(nodes, cols, data)
  }

  it("should support the same node label from multiple node tables") {
    // this creates additional :Person nodes
    val personsPart2 = caps.sparkSession.createDataFrame(
      Seq(
        (5L, false, "Soeren", 23L),
        (6L, false, "Hannes", 42L))
    ).toDF("ID", "IS_SWEDE", "NAME", "NUM")

    val personTable2 = CAPSNodeTable.fromMapping(personTable.mapping, personsPart2)

    val graph = caps.graphs.create(personTable, personTable2)
    graph.nodes("n").size shouldBe 6
  }

  it("should support the same relationship type from multiple relationship tables") {
    // this creates additional :KNOWS relationships
    val knowsParts2 = caps.sparkSession.createDataFrame(
      Seq(
        (1L, 7L, 2L, 2017L),
        (1L, 8L, 3L, 2016L))
    ).toDF("SRC", "ID", "DST", "SINCE")

    val knowsTable2 = CAPSRelationshipTable.fromMapping(knowsTable.mapping, knowsParts2)

    val graph = caps.graphs.create(personTable, knowsTable, knowsTable2)
    graph.relationships("r").size shouldBe 8
  }

  it("should return an empty result for non-present types") {
    val graph = caps.graphs.create(personTable, knowsTable)
    graph.nodes("n", CTNode("BAR")).size shouldBe 0
    graph.relationships("r", CTRelationship("FOO")).size shouldBe 0
  }

  it("should handle a single df containing multiple relationship types") {
    val yingYang = caps.sparkSession.createDataFrame(
      Seq(
        (1L, 8L, 3L, "HATES"),
        (1L, 3L, 4L, "HATES"),
        (2L, 4L, 3L, "LOVES"),
        (2L, 5L, 4L, "LOVES"),
        (3L, 6L, 4L, "LOVES"))
    ).toDF("SRC", "ID", "DST", "TYPE")

    val relMapping = RelationshipMapping
      .on("ID")
      .from("SRC")
      .to("DST")
      .withSourceRelTypeKey("TYPE", Set("HATES", "LOVES"))

    val relTable = CAPSRelationshipTable.fromMapping(relMapping, yingYang)

    val graph = caps.graphs.create(personTable, relTable)

    graph.relationships("l", CTRelationship("LOVES")).size shouldBe 3
    graph.relationships("h", CTRelationship("HATES")).size shouldBe 2
  }
}
