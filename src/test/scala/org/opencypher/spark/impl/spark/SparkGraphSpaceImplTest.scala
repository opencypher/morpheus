package org.opencypher.spark.impl.spark

import org.opencypher.spark.{TestSparkCypherSession, TestSuiteImpl}

// TODO: Reactivate graph space re-impl
class SparkGraphSpaceImplTest extends TestSuiteImpl with TestSparkCypherSession.Fixture {

//  test("import empty graph") {
//    val space = SparkGraphSpace.createEmpty(session)
//
//    val graph = space.importGraph("test", DataFrameGraph.empty)
//
//    space.graph("test") should equal(Some(graph))
//    graph._nodes("a", CTNode).data.count() should equal(0)
//    graph._relationships("r", CTRelationship).data.count() should equal(0)
//  }
//
//  test("import graph using node table") {
//    val space = SparkGraphSpace.createEmpty(session)
//
//    import sparkSession.implicits._
//
//    val nodes = List(
//      (1L, "Alice", 12.0d, false, true),
//      (2L, "Bob", 4.0d, true, false)
//    ).toDF("id", "name", "carat", "m", "f")
//
//    val graph = space.importGraph("test",
//      DataFrameGraphBuilder
//      .withNodesDF(nodes, 0)
//      .property("name", 1, CTString)
//      .property("carat", 2, CTFloat)
//      .label("Male", 3)
//      .label("Female", 4)
//      .build
//    )
//
//    space.graph("test") should equal(Some(graph))
//    graph._nodes("a", CTNode).data.count() should equal(2)
//  }
}

