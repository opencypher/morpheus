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
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.value.CAPSEntity
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{RecordsVerificationFixture, TeamDataFixture}
import CAPSEntity._

class CAPSGraphOperationsTest extends CAPSTestSuite with TeamDataFixture with RecordsVerificationFixture {

  test("union") {
    val graph1 = caps.graphs.create(personTable, knowsTable)
    val graph2 = caps.graphs.create(programmerTable, bookTable, readsTable)

    val result = graph1 unionAll graph2
    val nodeRecords = result.nodes("n")
    val n = Var("n")(CTNode)

    val nExprs = Seq(
      n,
      nHasLabelBook,
      nHasLabelPerson,
      nHasLabelProgrammer,
      nHasLabelSwedish,
      nHasPropertyLanguage,
      nHasPropertyLuckyNumber,
      nHasPropertyName,
      nHasPropertyTitle,
      nHasPropertyYear)

    verify(nodeRecords, nExprs, Bag(
      Row(1L.withPrefix(0).toList, false, true, false, true, null, 23L, "Mats", null, null),
      Row(2L.withPrefix(0).toList, false, true, false, false, null, 42L, "Martin", null, null),
      Row(3L.withPrefix(0).toList, false, true, false, false, null, 1337L, "Max", null, null),
      Row(4L.withPrefix(0).toList, false, true, false, false, null, 9L, "Stefan", null, null),
      Row(10L.withPrefix(1).toList, true, false, false, false, null, null, null, "1984", 1949L),
      Row(20L.withPrefix(1).toList, true, false, false, false, null, null, null, "Cryptonomicon", 1999L),
      Row(30L.withPrefix(1).toList, true, false, false, false, null, null, null, "The Eye of the World", 1990L),
      Row(40L.withPrefix(1).toList, true, false, false, false, null, null, null, "The Circle", 2013L),
      Row(100L.withPrefix(1).toList, false, true, true, false, "C", 42L, "Alice", null, null),
      Row(200L.withPrefix(1).toList, false, true, true, false, "D", 23L, "Bob", null, null),
      Row(300L.withPrefix(1).toList, false, true, true, false, "F", 84L, "Eve", null, null),
      Row(400L.withPrefix(1).toList, false, true, true, false, "R", 49L, "Carl", null, null)
    ))

    val relRecords = result.relationships("r")

    val rExprs = Seq(rStart,
    r,
    rHasTypeKnows,
    rHasTypeReads,
    rEnd,
    rHasPropertyRecommends,
    rHasPropertySince)

    verify(relRecords, rExprs, Bag(
      Row(1L.withPrefix(0).toList, 1L.withPrefix(0).toList, true, false, 2L.withPrefix(0).toList, null, 2017L),
      Row(1L.withPrefix(0).toList, 2L.withPrefix(0).toList, true, false, 3L.withPrefix(0).toList, null, 2016L),
      Row(1L.withPrefix(0).toList, 3L.withPrefix(0).toList, true, false, 4L.withPrefix(0).toList, null, 2015L),
      Row(2L.withPrefix(0).toList, 4L.withPrefix(0).toList, true, false, 3L.withPrefix(0).toList, null, 2016L),
      Row(2L.withPrefix(0).toList, 5L.withPrefix(0).toList, true, false, 4L.withPrefix(0).toList, null, 2013L),
      Row(3L.withPrefix(0).toList, 6L.withPrefix(0).toList, true, false, 4L.withPrefix(0).toList, null, 2016L),
      Row(100L.withPrefix(1).toList, 100L.withPrefix(1).toList, false, true, 10L.withPrefix(1).toList, true, null),
      Row(200L.withPrefix(1).toList, 200L.withPrefix(1).toList, false, true, 40L.withPrefix(1).toList, true, null),
      Row(300L.withPrefix(1).toList, 300L.withPrefix(1).toList, false, true, 30L.withPrefix(1).toList, true, null),
      Row(400L.withPrefix(1).toList, 400L.withPrefix(1).toList, false, true, 20L.withPrefix(1).toList, false, null)
    ))

  }
}
