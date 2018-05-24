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

import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.execution.UnionExec
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.impl.util.SparkQueryPlanCostEstimation._
import org.opencypher.spark.testing.CAPSTestSuite

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
    val cost = optimizedPlan.cost
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
    val cost = optimizedPlan.cost
    cost shouldBe 348L
  }

  it("not aliasing column names enables plan reuse") {
    import sparkSession.implicits._

    sparkSession.sessionState.conf.setConfString("spark.sql.inMemoryColumnarStorage.batchSize", "1")

    val inputDF = sparkSession.createDataFrame(Seq((0L, 1L))).toDF("a", "b")
    val df0 = sparkSession.createDataFrame(Seq((0L, 1L))).toDF("c", "d")

    val df1 = inputDF.filter($"a" === 0)
    val df2 = inputDF.filter($"a" === 0)

    val joinedDf0 = df1.join(df0, $"a" === $"c")
    val joinedDf1 = df2.join(df0, $"a" === $"c")

    val res = joinedDf0.union(joinedDf1)

    val optPlan = res.queryExecution.optimizedPlan
    val sparkPlan = res.queryExecution.sparkPlan

    optPlan.foreach {
      case u@Union(Seq(left, right)) =>
//        println(s"*** Logical ****\n$u")
        left shouldEqual right
      case _ =>
    }

    sparkPlan.foreach {
      case u@UnionExec(Seq(left, right)) =>
//        println(s"*** Physical **** \n$u")
        left shouldEqual right
      case _ =>
    }

    optPlan.cost shouldBe 864L

  }

  it("aliasing column names undermines plan reuse") {
    import sparkSession.implicits._

    val inputDF = sparkSession.createDataFrame(Seq((0L, 1L))).toDF("a", "b")
    val df0 = sparkSession.createDataFrame(Seq((0L, 1L))).toDF("c", "d")

    val df1 = inputDF.filter($"a" === 0)
    val df2 = inputDF.filter($"a" === 0)

    val joinedDf0 = df1.join(df0, $"a" === $"c")

    val df3 = df0.withColumnRenamed("c", "e")

    val joinedDf1 = df2.join(df3, $"a" === $"e")

    val res = joinedDf0.union(joinedDf1)

    val optPlan = res.queryExecution.optimizedPlan
    val sparkPlan = res.queryExecution.sparkPlan

    optPlan.foreach {
      case u@Union(Seq(left, right)) =>
//        println(s"*** Logical ****\n$u")
        left shouldNot equal(right)
      case _ =>
    }

    sparkPlan.foreach {
      case u@UnionExec(Seq(left, right)) =>
//        println(s"*** Physical ****\n$u")
        left shouldNot equal(right)
      case _ =>
    }

    optPlan.cost shouldBe 1136L
  }

}
