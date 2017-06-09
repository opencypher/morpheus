package org.opencypher.spark_legacy.benchmark

import org.apache.spark.sql.functions.col
import org.opencypher.spark_legacy.impl.{FixedLengthPattern, Out}
import org.opencypher.spark.{TestSuiteImpl, TestSparkCypherSession}

class TripletBenchmarksTest extends TestSuiteImpl with TestSparkCypherSession.Fixture {

  case class NodeRow(id: Long, group: Boolean, company: Boolean)
  case class TripletRow(id: Long, startId: Long, endId: Long, group: Boolean, company: Boolean)

  test("query example") {
    sparkSession.sparkContext.setLogLevel("OFF")

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

    val nodes = sparkSession.createDataFrame(Seq(n0, n1, n2, n3, n4))
    val outRels = sparkSession.createDataFrame(Seq(rEnd0, rEnd1, rEnd2, rEnd3, rEnd4, rEnd5))
    val inRels = sparkSession.createDataFrame(Seq(rStart0, rStart1, rStart2, rStart3, rStart4, rStart5))

    val g = TripletGraph(Map("Group" -> nodes.filter(col("group"))), Map("FOO" -> (outRels -> inRels)))
    val b = TripletBenchmarks(FixedLengthPattern("Group", Seq(Out("FOO") -> "Company")))

    b.run(g).computeCount shouldBe 2
    b.run(g).computeChecksum shouldBe 3 ^ 4
  }

}
