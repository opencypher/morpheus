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
package org.opencypher.spark.testing.support

import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.testing.fixture.{CAPSSessionFixture, SparkSessionFixture}
import org.scalatest.Assertion

import scala.collection.immutable.Map

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture with CAPSSessionFixture =>

  private def getEntityIds(records: CAPSRecords): Set[Long] = {
    val entityVar = records.header.vars.toSeq match {
      case Seq(v) => v
      case other => throw new UnsupportedOperationException(s"Expected records with 1 entity, got $other")
    }

    records.df.select(records.header.column(entityVar)).collect().map(_.getLong(0)).toSet
  }

  private def verify(actual: CAPSGraph, expected: CAPSGraph): Assertion = {
    val expectedNodeIds = getEntityIds(expected.nodes("n"))
    val expectedRelIds = getEntityIds(expected.relationships("r"))

    val actualNodeIds = getEntityIds(actual.nodes("n"))
    val actualRelIds = getEntityIds(actual.relationships("r"))

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
