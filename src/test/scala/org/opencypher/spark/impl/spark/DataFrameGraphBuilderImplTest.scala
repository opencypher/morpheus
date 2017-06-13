package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.TestSuiteImpl
import org.opencypher.spark.api.types.{CTFloat, CTString}
import org.scalatest.mockito.MockitoSugar

class DataFrameGraphBuilderImplTest extends TestSuiteImpl with MockitoSugar {

  def start = new DataFrameGraphBuilderImpl(DataFrameGraph(None, None))
  
  val nodes = mock[DataFrame]

  test("build empty graph") {
    val graph = start.build

    graph.verify should equal(DataFrameGraph(None, None).verify)
  }

  test("build graph with simple node table") {
    val graph = start
      .withNodesDF(nodes, 0)
      .property("name", 1, CTString)
      .property("carat", 2, CTFloat)
      .label("Male", 3)
      .label("Female", 4)
      .build

    graph.verify should equal(
      DataFrameGraph(
        Some(NodeDataFrame(nodes, 0, Map("Male" -> 3, "Female" -> 4), Map("name" -> (1, CTString), "carat" -> (2, CTFloat)))),
        None
      ).verify
    )
  }

  test("refuse to add the same property twice") {
    a[IllegalArgumentException] shouldBe thrownBy {
      start.withNodesDF(nodes, 0).property("name", 1, CTString).property("name", 2, CTFloat).build
    }
  }

  test("refuse to add the same label twice") {
    a[IllegalArgumentException] shouldBe thrownBy {
      start.withNodesDF(nodes, 0).label("Person", 1).label("Person", 3).build
    }
  }
}
