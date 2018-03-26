package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.test.fixture.MiniDFSClusterFixture

trait CatalogDDLBehaviour extends MiniDFSClusterFixture {
  self: AcceptanceTest =>

  def catalogDDLBehaviour(initGraph: String => CAPSGraph): Unit = {
    it("supports CREATE GRAPH") {
      val inputGraph = initGraph(
        """
          |CREATE (:A)
        """.stripMargin)

      caps.store(GraphName("foo"), inputGraph)

      val result = caps.cypher(
        """
          |CREATE GRAPH session.bar {
          | From GRAPH session.foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      val sessionSource = caps.dataSource(caps.sessionNamespace)
      sessionSource.hasGraph(GraphName("bar")) shouldBe true
      sessionSource.graph(GraphName("bar")) shouldEqual inputGraph
      result.graph shouldBe None
      result.records shouldBe None
    }

    it("can store to external graph source") {
      val csvNamespace = Namespace("csv")
      val inputGraphName = GraphName("foo")
      val outputGraphName = GraphName("test")

      val hdfsGraphSource = HdfsCsvPropertyGraphDataSource(clusterConfig, "/")
      caps.registerSource(csvNamespace, hdfsGraphSource)

      val inputGraph = initGraph(
        """
          |CREATE (:A)-[:B]->(:C)
        """.stripMargin)
      caps.store(inputGraphName, inputGraph)

      val result = caps.cypher(
        """
          |CREATE GRAPH csv.test {
          | From GRAPH session.foo
          | RETURN GRAPH
          |}
        """.stripMargin)

      caps.dataSource(csvNamespace).hasGraph(outputGraphName) shouldBe true
      caps.dataSource(csvNamespace).graph(outputGraphName).nodes("n").size should be 2
    }
  }
}