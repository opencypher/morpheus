package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.impl.CAPSGraph

trait CatalogDDLBehaviour {
  self: AcceptanceTest =>

  def catalogDDLBehaviour(initGraph: String => CAPSGraph): Unit = {
    describe("CREATE GRAPH") {
      it("supports CREATE GRAPH on the session") {
        val inputGraph = initGraph(
          """
            |CREATE (:A)
          """.stripMargin)

        caps.store(GraphName("foo"), inputGraph)

        val result = caps.cypher(
          """
            |CREATE GRAPH bar {
            | From GRAPH foo
            | RETURN GRAPH
            |}
          """.stripMargin)

        val sessionSource = caps.dataSource(caps.sessionNamespace)
        sessionSource.hasGraph(GraphName("bar")) shouldBe true
        sessionSource.graph(GraphName("bar")) shouldEqual inputGraph
        result.graph shouldBe None
        result.records shouldBe None
      }
    }

    describe("DELETE GRAPH") {
      it("can delete a session graph") {
        val graphName = GraphName("foo")
        caps.store(graphName, initGraph("CREATE (:A)"))

        val result = caps.cypher(
          """
            |DELETE GRAPH session.foo
          """.stripMargin
        )

        caps.dataSource(caps.sessionNamespace).hasGraph(graphName) shouldBe false
        result.graph shouldBe None
        result.records shouldBe None
      }
    }
  }
}