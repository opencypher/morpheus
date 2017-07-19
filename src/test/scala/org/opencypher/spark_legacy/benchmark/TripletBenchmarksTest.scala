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
package org.opencypher.spark_legacy.benchmark

import org.apache.spark.sql.functions.col
import org.opencypher.spark_legacy.impl.{FixedLengthPattern, Out}
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}

class TripletBenchmarksTest extends BaseTestSuite with SparkTestSession.Fixture {

  case class NodeRow(id: Long, group: Boolean, company: Boolean)
  case class TripletRow(id: Long, startId: Long, endId: Long, group: Boolean, company: Boolean)

  test("query example") {
    session.sparkContext.setLogLevel("OFF")

    val n0 = NodeRow(0, group = true, company = false)
    val n1 = NodeRow(1, group = true, company = false)
    val n2 = NodeRow(2, group = true, company = false)
    val n3 = NodeRow(3, group = false, company = false)
    val n4 = NodeRow(4, group = false, company = true)

    val rStart0 = TripletRow(0, 0, 1, group = true, company = false)
    val rStart1 = TripletRow(1, 1, 1, group = true, company = false)
    val rStart2 = TripletRow(2, 1, 1, group = true, company = false)
    val rStart3 = TripletRow(3, 1, 4, group = false, company = true)
    val rStart4 = TripletRow(4, 2, 4, group = false, company = true)
    val rStart5 = TripletRow(5, 3, 4, group = false, company = false)

    val rEnd0 = TripletRow(0, 0, 1, group = true, company = false) // not in result
    val rEnd1 = TripletRow(1, 1, 1, group = true, company = false) // not
    val rEnd2 = TripletRow(2, 1, 1, group = true, company = false) // not
    val rEnd3 = TripletRow(3, 1, 4, group = false, company = true) // include
    val rEnd4 = TripletRow(4, 2, 4, group = false, company = true) // include
    val rEnd5 = TripletRow(5, 3, 4, group = false, company = true) // not include

    val nodes = session.createDataFrame(Seq(n0, n1, n2, n3, n4))
    val outRels = session.createDataFrame(Seq(rEnd0, rEnd1, rEnd2, rEnd3, rEnd4, rEnd5))
    val inRels = session.createDataFrame(Seq(rStart0, rStart1, rStart2, rStart3, rStart4, rStart5))

    val g = TripletGraph(Map("Group" -> nodes.filter(col("group"))), Map("FOO" -> (outRels -> inRels)))
    val b = TripletBenchmarks(FixedLengthPattern("Group", Seq(Out("FOO") -> "Company")))

    b.run(g).computeCount shouldBe 2
    b.run(g).computeChecksum shouldBe 3 ^ 4
  }

}
