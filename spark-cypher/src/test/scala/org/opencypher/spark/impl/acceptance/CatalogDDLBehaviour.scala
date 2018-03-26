package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.impl.CAPSGraph

trait CatalogDDLBehaviour {
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
  }
}