package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.{CypherSession, GraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.PGDSAcceptance
import org.opencypher.okapi.testing.propertygraph.TestGraph
import org.opencypher.spark.test.fixture.{CAPSSessionFixture, Neo4jServerFixture, SparkSessionFixture}

class Neo4jPGDSAcceptanceTest extends PGDSAcceptance with SparkSessionFixture with CAPSSessionFixture with Neo4jServerFixture {

  override def initSession(): CypherSession = caps

  override def create(graphName: GraphName, testGraph: TestGraph, createStatements: String): PropertyGraphDataSource = {
    new CommunityNeo4jGraphDataSource(neo4jConfig)
  }

  override def dataFixture: String = createStatements
}
