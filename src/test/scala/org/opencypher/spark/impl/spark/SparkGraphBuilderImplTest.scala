package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTFloat, CTString}
import org.scalatest.mockito.MockitoSugar

class SparkGraphBuilderImplTest extends StdTestSuite with MockitoSugar {

  def start = new SparkGraphBuilderImpl(ExternalGraph(None, None))
  val nodes = mock[DataFrame]

  test("build empty graph") {
    val graph = start.done

    graph.verify should equal(ExternalGraph(None, None).verify)
  }

  test("build graph with simple node table") {
    val graph = start
      .withNodesDF(nodes, 0)
      .property("name", 1, CTString)
      .property("carat", 2, CTFloat)
      .label("Male", 3)
      .label("Female", 4)
      .done

    graph.verify should equal(
      ExternalGraph(
        Some(ExternalNodes(nodes, 0, Map("Male" -> 3, "Female" -> 4), Map("name" -> (1, CTString), "carat" -> (2, CTFloat)))),
        None
      ).verify
    )
  }

  test("refuse to add the same property twice") {
    a[IllegalArgumentException] shouldBe thrownBy {
      start.withNodesDF(nodes, 0).property("name", 1, CTString).property("name", 2, CTFloat).done
    }
  }

  test("refuse to add the same label twice") {
    a[IllegalArgumentException] shouldBe thrownBy {
      start.withNodesDF(nodes, 0).label("Person", 1).label("Person", 3).done
    }
  }
}
