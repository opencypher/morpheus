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
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.TeamDataFixture

import scala.collection.Bag

class CAPSGraphOperationsTest extends CAPSTestSuite with TeamDataFixture {

  test("union") {
    val graph1 = CAPSGraph.create(personTable, knowsTable)
    val graph2 = CAPSGraph.create(programmerTable, bookTable, readsTable)

    val result = graph1 unionAll graph2

    val nodes = result.nodes("n").toDF().collect.toBag
    nodes should equal(
      Bag(
        Row(1L, false, true, false, true, null, 23L, "Mats", null, null),
        Row(2L, false, true, false, false, null, 42L, "Martin", null, null),
        Row(3L, false, true, false, false, null, 1337L, "Max", null, null),
        Row(4L, false, true, false, false, null, 9L, "Stefan", null, null),
        Row(10L, true, false, false, false, null, null, null, "1984", 1949L),
        Row(20L, true, false, false, false, null, null, null, "Cryptonomicon", 1999L),
        Row(30L, true, false, false, false, null, null, null, "The Eye of the World", 1990L),
        Row(40L, true, false, false, false, null, null, null, "The Circle", 2013L),
        Row(100L, false, true, true, false, "C", 42L, "Alice", null, null),
        Row(200L, false, true, true, false, "D", 23L, "Bob", null, null),
        Row(300L, false, true, true, false, "F", 84L, "Eve", null, null),
        Row(400L, false, true, true, false, "R", 49L, "Carl", null, null)
      )
    )

    val rels = result.relationships("r").toDF().collect.toBag
    rels should equal(
      Bag(
        Row(1L, 1L, "KNOWS", 2L, null, 2017L),
        Row(1L, 2L, "KNOWS", 3L, null, 2016L),
        Row(1L, 3L, "KNOWS", 4L, null, 2015L),
        Row(2L, 4L, "KNOWS", 3L, null, 2016L),
        Row(2L, 5L, "KNOWS", 4L, null, 2013L),
        Row(3L, 6L, "KNOWS", 4L, null, 2016L),
        Row(100L, 100L, "READS", 10L, true, null),
        Row(200L, 200L, "READS", 40L, true, null),
        Row(300L, 300L, "READS", 30L, true, null),
        Row(400L, 400L, "READS", 20L, false, null)
      )
    )
  }
}


