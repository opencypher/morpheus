package org.opencypher.caps.api.io

import org.apache.spark.sql.Row
import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.io.hdfs.CsvGraphLoader
import org.opencypher.caps.api.spark.CAPSGraph

class CsvGraphLoaderAcceptanceTest extends CAPSTestSuite {

  test("load csv graph") {
    val loader = new CsvGraphLoader(getClass.getResource("/csv_graph").toString)
    val graph: CAPSGraph = loader.load
    graph.nodes("n").details.toDF().collect().toSet should equal(Set(
      Row( 1, true,  true, false, true, "Stephan",   42),
      Row( 2, false, true,  true, true,    "Mats",   23),
      Row( 3, true,  true, false, true,  "Martin", 1337),
      Row( 4, true,  true, false, true,     "Max",    8)
    ))

    graph.relationships("rel").details.toDF().collect.toSet should equal(Set(
      Row(1, 10, 0, 2, 2016),
      Row(2, 20, 0, 3, 2017),
      Row(3, 30, 0, 4, 2015)
    ))
  }
}
