package org.opencypher.caps.api.io.hdfs

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.{CAPSTestSession, HDFSTestSession, SparkTestSession}
import org.scalatest.{FunSuite, Matchers}

class CsvGraphLoaderAcceptanceTest extends FunSuite
  with CAPSTestSession.Fixture
  with SparkTestSession.Fixture
  with HDFSTestSession.Fixture
  with Matchers {

  test("load csv graph") {
    val loader = new CsvGraphLoader(hdfsURI.toString, session.sparkContext.hadoopConfiguration)

    val graph: CAPSGraph = loader.load
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }
}
