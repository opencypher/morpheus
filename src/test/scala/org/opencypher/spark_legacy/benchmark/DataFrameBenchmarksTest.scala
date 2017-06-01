package org.opencypher.spark_legacy.benchmark

import org.apache.spark.sql.functions.col
import org.opencypher.spark_legacy.impl.{FixedLengthPattern, Out}
import org.opencypher.spark.{TestSuiteImpl, TestSession}

class DataFrameBenchmarksTest extends TestSuiteImpl with TestSession.Fixture {

  case class NodeRow(id: Long, group: Boolean, company: Boolean)
  case class RelRow(id: Long, startId: Long, endId: Long, typ: String = "ALLOWED_INHERIT")

  test("query example") {
    session.sparkContext.setLogLevel("OFF")

    val n0 = NodeRow(0, group = true, company = false)
    val n1 = NodeRow(1, group = true, company = false)
    val n2 = NodeRow(2, group = true, company = false)
    val n3 = NodeRow(3, group = false, company = false)
    val n4 = NodeRow(4, group = false, company = true)

    val r0 = RelRow(0, 0, 1) // not in result
    val r1 = RelRow(1, 1, 1) // not
    val r2 = RelRow(2, 1, 1) // not
    val r3 = RelRow(3, 1, 4) // include
    val r4 = RelRow(4, 2, 4) // include
    val r5 = RelRow(5, 3, 4) // not include

    val nodes = session.createDataFrame(Seq(n0, n1, n2, n3, n4))
    val rels = session.createDataFrame(Seq(r0, r1, r2, r3, r4, r5))

    val g = SimpleDataFrameGraph(
      Map("Group" -> nodes.filter(col("group")), "Company" -> nodes.filter(col("company"))),
      Map("FOO" -> (rels -> rels))
    )
    val b = DataFrameBenchmarks(FixedLengthPattern("Group", Seq(Out("FOO") -> "Company")))

    b.run(g).computeCount shouldBe 2
    b.run(g).computeChecksum shouldBe 3 ^ 4
  }

}
