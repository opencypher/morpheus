package org.opencypher.caps.api.io

import org.opencypher.caps.CAPSTestSuiteWithHDFS
import org.opencypher.caps.api.io.hdfs.CsvGraphLoader
import org.opencypher.caps.api.spark.CAPSGraph

class CsvGraphLoaderAcceptanceTest extends CAPSTestSuiteWithHDFS {

  test("load csv graph") {
    val loader = new CsvGraphLoader(hdfsURI.toString, sparkSession.sparkContext.hadoopConfiguration)

    val graph: CAPSGraph = loader.load
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }
}
