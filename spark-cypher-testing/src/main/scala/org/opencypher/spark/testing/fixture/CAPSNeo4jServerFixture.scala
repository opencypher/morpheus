package org.opencypher.spark.testing.fixture

import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.testing.support.creation.CAPSNeo4jHarnessUtils._

trait CAPSNeo4jServerFixture extends Neo4jServerFixture {
  self: BaseTestSuite =>

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    neo4jServer.withSchemaProcedure
  }
}
