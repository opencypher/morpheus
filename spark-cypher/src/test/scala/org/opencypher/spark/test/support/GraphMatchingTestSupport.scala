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
package org.opencypher.spark.test.support

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.test.fixture.{CAPSSessionFixture, SparkSessionFixture}
import org.scalatest.Assertion

import scala.collection.immutable.Map

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture with CAPSSessionFixture =>

  private def verify(actual: CAPSGraph, expected: CAPSGraph): Assertion = {
    val expectedNodeIds = expected.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
    val expectedRelIds = expected.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet
    val actualNodeIds = actual.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
    val actualRelIds = actual.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

    expectedNodeIds should equal(actualNodeIds)
    expectedRelIds should equal(actualRelIds)
  }

  implicit class GraphsMatcher(graphs: Map[String, CAPSGraph]) {
    def shouldMatch(expectedGraphs: CAPSGraph*): Unit = {
      withClue("expected and actual must have same size") {
        graphs.size should equal(expectedGraphs.size)
      }

      graphs.values.zip(expectedGraphs).foreach {
        case (actual, expected) => verify(actual, expected)
      }
    }
  }

  implicit class GraphMatcher(graph: CAPSGraph) {
    def shouldMatch(expectedGraph: CAPSGraph): Unit = verify(graph, expectedGraph)
  }
}
