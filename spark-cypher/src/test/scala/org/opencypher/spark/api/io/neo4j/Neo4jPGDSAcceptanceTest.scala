package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.{CypherSession, GraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.PGDSAcceptance
import org.opencypher.okapi.testing.propertygraph.TestGraph
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.Neo4jServerFixture

class Neo4jPGDSAcceptanceTest extends CAPSTestSuite with Neo4jServerFixture with PGDSAcceptance {

  override def initSession(): CypherSession = caps

  override def create(graphName: GraphName, testGraph: TestGraph, createStatements: String): PropertyGraphDataSource = {
    new CommunityNeo4jGraphDataSource(neo4jConfig, Map(graphName -> CommunityNeo4jGraphDataSource.defaultQuery))
  }

  override def dataFixture: String = createStatements
}
