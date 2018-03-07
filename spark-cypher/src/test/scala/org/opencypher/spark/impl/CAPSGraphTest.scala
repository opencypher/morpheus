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
 */
package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{GraphCreationFixture, TeamDataFixture}

import scala.collection.Bag

abstract class CAPSGraphTest extends CAPSTestSuite with GraphCreationFixture with TeamDataFixture {

  it("should return only nodes with that exact label") {
    val graph = initGraph(dataFixtureWithoutArrays)

    val nodes = graph.nodesWithExactLabels("n", Set("Person"))

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(4L, true, 8L, "Donald")
      ))
  }
}
