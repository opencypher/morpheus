package org.opencypher.spark.impl.spark

import org.opencypher.spark.api.spark.SparkGraphSpace
import org.opencypher.spark.api.types.{CTFloat, CTNode, CTRelationship, CTString}
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkGraphBuilderImplTest extends StdTestSuite with TestSession.Fixture {

  test("construct graph builder for empty graph") {
    val space = SparkGraphSpace.createEmpty(session)
    val builder = new SparkGraphBuilderImpl(space)
    val graph = builder.graph

    graph._nodes("a", CTNode).data.count() should equal(0)
    graph._relationships("r", CTRelationship).data.count() should equal(0)
  }

  test("construct graph using node table") {
    val space = SparkGraphSpace.createEmpty(session)

    import session.implicits._

    val nodes = List(
      (1L, "Alice", 12.0d, false, true),
      (2L, "Bob", 4.0d, true, false)
    ).toDF("id", "name", "carat", "m", "f")

    val builder = new SparkGraphBuilderImpl(space)
      .withNodesDF(nodes, 0)
      .property("name", 1, CTString)
      .property("carat", 2, CTFloat)
      .label("Male", 3)
      .label("Female", 4)

    val graph = builder.graph

    graph._nodes("a", CTNode).data.count() should equal(2)
  }
}

