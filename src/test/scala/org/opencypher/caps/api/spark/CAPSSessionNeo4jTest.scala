package org.opencypher.caps.api.spark

import java.net.{URI, URLEncoder}

import org.opencypher.caps.api.io.neo4j.Neo4jGraphSource
import org.opencypher.caps.{Neo4jTestSession, SparkTestSession}
import org.scalatest.{FunSuite, Matchers}

class CAPSSessionNeo4jTest extends FunSuite
  with SparkTestSession.Fixture
  with Neo4jTestSession.Fixture
  with Matchers {

  test("Neo4j via URI") {
    val capsSession = CAPSSession.builder(session).get

    val nodeQuery = URLEncoder.encode("MATCH (n) RETURN n", "UTF-8")
    val relQuery = URLEncoder.encode("MATCH ()-[r]->() RETURN r", "UTF-8")
    val uri = URI.create(s"$neo4jHost?$nodeQuery;$relQuery")

    val graph = capsSession.withGraphAt(uri)
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }

  test("Neo4j via mount point") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/neo4j1", Neo4jGraphSource(neo4jConfig, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r"))
      .get

    val graph = capsSession.withGraphAt(URI.create("/neo4j1"))
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }

  test("Neo4j via mounted URI") {
    val capsSession = CAPSSession
      .builder(session)
      .withGraphSource(Neo4jGraphSource(neo4jConfig, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r"))
      .get

    val graph = capsSession.withGraphAt(neo4jServer.boltURI())
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }
}
