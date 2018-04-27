package org.opencypher.spark.api.io

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.testing.{BaseTestSuite, PGDSAcceptance}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._

import scala.util.{Failure, Success, Try}

trait CAPSPGDSAcceptance extends PGDSAcceptance[CAPSSession] {
  self: BaseTestSuite =>

  it("supports storing of graphs with tags, query variant") {
    cypherSession.cypher("CREATE GRAPH g1 { CONSTRUCT NEW () RETURN GRAPH }")
    cypherSession.cypher("CREATE GRAPH g2 { CONSTRUCT NEW () RETURN GRAPH }")

    Try(cypherSession.cypher(s"CREATE GRAPH $ns.g3 { CONSTRUCT ON g1, g2 RETURN GRAPH }")) match {
      case Failure(_: UnsupportedOperationException) =>
      case Failure(t) => badFailure(t)
      case Success(_) =>
        val graph = cypherSession.cypher(s"FROM GRAPH $ns.g3 RETURN GRAPH").getGraph.asCaps

        withClue("tags should be restored correctly") {
          graph.tags should equal(Set(0, 1))
        }
        graph.nodes("n").collect.length shouldBe 2
    }
  }

  it("supports storing of graphs with tags, API variant") {
    cypherSession.cypher("CREATE GRAPH g1 { CONSTRUCT NEW () RETURN GRAPH }")
    cypherSession.cypher("CREATE GRAPH g2 { CONSTRUCT NEW () RETURN GRAPH }")

    val graphToStore = cypherSession.cypher("CONSTRUCT ON g1, g2 RETURN GRAPH").getGraph.asCaps

    val name = GraphName("g3")

    Try(cypherSession.catalog.source(ns).store(name, graphToStore)) match {
      case Failure(_: UnsupportedOperationException) =>
      case Failure(t) => badFailure(t)
      case Success(_) =>
        val graphRead = cypherSession.catalog.source(ns).graph(name).asCaps

        withClue("tags should be restored correctly") {
          graphRead.tags should equal(graphToStore.tags)
          graphRead.tags should equal(Set(0, 1))
        }
        graphRead.nodes("n").collect.length shouldBe 2
    }
  }


}
