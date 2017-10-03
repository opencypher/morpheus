/**
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
package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.record.NodeScan
import org.opencypher.caps.test.CAPSTestSuite

class UnionGraphTest extends CAPSTestSuite {
  import CAPSGraphTestData._

  test("Node scan from single node CAPSRecords") {
    val inputGraph = TestGraph(`:Person`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = UnionGraph(CAPSGraph.create(inputNodes, inputGraph.schema))
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal (Set(
      Row(0, true, true,    "Mats",   23),
      Row(1, true, false, "Martin",   42),
      Row(2, true, false,    "Max", 1337),
      Row(3, true, false, "Stefan",    9)
    ))
  }

  test("Node scan from multiple single node CAPSRecords") {
    val unionGraph = UnionGraph(TestGraph(`:Person`).graph, TestGraph(`:Book`).graph)
    val outputNodes = unionGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_yearINTEGER",
      "____n_dot_titleSTRING"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0, true,  true,  false,   "Mats",   23, null,                   null),
      Row(1, true,  false, false, "Martin",   42, null,                   null),
      Row(2, true,  false, false,    "Max", 1337, null,                   null),
      Row(3, true,  false, false, "Stefan",    9, null,                   null),
      Row(0, false, false, true,      null, null, 1949,                 "1984"),
      Row(1, false, false, true,      null, null, 1999,        "Cryptonomicon"),
      Row(2, false, false, true,      null, null, 1990, "The Eye of the World"),
      Row(3, false, false, true,      null, null, 2013,           "The Circle")
    ))
  }

  // TODO: Fix TODO in nodeFromSchema
  test("Returns only distinct results") {
    val inputGraph = TestGraph(`:Person`).graph
    val inputNodes = inputGraph.nodes("n")
    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val nodeScan = NodeScan.on("n" -> "ID") {
        _.build
          .withImpliedLabel("Person")
          .withOptionalLabel("Swedish" -> "IS_SWEDE")
          .withPropertyKey("name" -> "NAME")
          .withPropertyKey("luckyNumber" -> "NUM")
      }
        .from(CAPSRecords.create(
          Seq("ID", "IS_SWEDE", "NAME", "NUM"),
          Seq(
            (0L, true, "Mats", 23L),
            (1L, false, "Martin", 42L),
            (2L, false, "Max", 1337L),
            (3L, false, "Stefan", 9L))
        ))
    val scanGraph = CAPSGraph.create(nodeScan)

    val unionGraph = UnionGraph(patternGraph, scanGraph)
    val outputNodes = unionGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0, true, true, "Mats", 23),
      Row(1, true, false, "Martin", 42),
      Row(2, true, false, "Max", 1337),
      Row(3, true, false, "Stefan", 9)
    ))
  }

  private def initPersonReadsBookGraph: CAPSGraph = {
    UnionGraph(
      TestGraph(`:READS`).graph,
      UnionGraph(TestGraph(`:Book`).graph,
        UnionGraph(TestGraph(`:Person`).graph)))
  }
}
