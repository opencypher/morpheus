package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{GraphCreationFixture, TeamDataFixture}

import scala.collection.Bag

abstract class CAPSGraphTest extends CAPSTestSuite with GraphCreationFixture with TeamDataFixture {

  it("should return only nodes with that exact label") {
    val graph = initGraph(dataFixtureWithoutArrays)

    val nodes = graph.nodesWithExactLabels("n", Set("Person"))

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(4L, true, 8L, "Donald")
      ))
  }
}
