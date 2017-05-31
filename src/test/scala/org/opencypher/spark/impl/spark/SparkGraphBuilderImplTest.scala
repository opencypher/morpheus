package org.opencypher.spark.impl.spark

import org.opencypher.spark.{StdTestSuite, TestSession}
import org.opencypher.spark.api.spark.SparkGraphSpace
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

class SparkGraphBuilderImplTest extends StdTestSuite with TestSession.Fixture {

  test("Can construct graph builder for empty graph") {
    val space = SparkGraphSpace.createEmpty(session)
    val builder = new SparkGraphBuilderImpl(space)
    val graph = builder.graph

    graph._nodes("a", CTNode).data.count() should equal(0)
    graph._relationships("r", CTRelationship).data.count() should equal(0)
  }
}

