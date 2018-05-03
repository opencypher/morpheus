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
package org.opencypher.spark.impl.metrics

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.test.CAPSTestSuite

class QueryMetrics extends CAPSTestSuite with DefaultGraphInit {

  it("generates a query plan of the expected cost") {
    val graph = initGraph(
      """
        |CREATE (a:A { name: 'A' })
        |CREATE (b:B { prop: 'B' })
        |CREATE (combo:A:B { name: 'COMBO', size: 2 })
        |CREATE (a)-[:R { since: 2004 }]->(b)
        |CREATE (b)-[:R { since: 2005 }]->(combo)
        |CREATE (combo)-[:S { since: 2006 }]->(combo)"""
        .stripMargin)
    val records = graph.cypher(
      """
        |MATCH (a)-[r1]->(b)-[r2]->(c)
        |RETURN r1.since, r2.since, type(r2)"""
        .stripMargin).getRecords
    val df = records.asCaps.data
    val optimizedPlan = df.queryExecution.optimizedPlan
    val cost = computeCostMetric(optimizedPlan)
    cost shouldBe 125995727L
  }

  it("generates a query plan for a really simple query") {
    val graph = initGraph(
      """
        |CREATE (a:A { name: 'A' })
        |CREATE (b:B { prop: 'B' })
        |CREATE (combo:A:B { name: 'COMBO', size: 2 })
        |CREATE (a)-[:R { since: 2004 }]->(b)
        |CREATE (b)-[:R { since: 2005 }]->(combo)
        |CREATE (combo)-[:S { since: 2006 }]->(combo)"""
        .stripMargin)
    val records = graph.cypher(
      """
        |MATCH (a)
        |RETURN a"""
        .stripMargin).getRecords
    val df = records.asCaps.data
    val optimizedPlan = df.queryExecution.optimizedPlan
    val cost = computeCostMetric(optimizedPlan)
    println(s"plan cost = $cost")
    cost shouldBe 348L
  }

  def computeCostMetric(plan: LogicalPlan): Long = {
    val sizeInMb = plan.map { child =>
      val stats = child.stats(session.sessionState.conf)
      stats.sizeInBytes
    }.sum
    sizeInMb.toLong
  }

}
