package org.opencypher.okapi.neo4j.io.testing

import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.okapi.testing.{BaseTestFixture, BaseTestSuite}

trait Neo4jServerFixture extends BaseTestFixture {
  self: BaseTestSuite =>

  var neo4jServer: ServerControls = _

  def neo4jConfig =
    Neo4jConfig(neo4jServer.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)

  def neo4jHost: String = {
    val scheme = neo4jServer.boltURI().getScheme
    val userInfo = s"${neo4jConfig.user}:${neo4jConfig.password.get}@"
    val host = neo4jServer.boltURI().getAuthority
    s"$scheme://$userInfo$host"
  }

  def userFixture: String = "CALL dbms.security.createUser('anonymous', 'password', false)"

  def dataFixture: String

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    neo4jServer = TestServerBuilders
      .newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture(userFixture)
      .withFixture(dataFixture)
      .newServer()
  }

  abstract override def afterAll(): Unit = {
    neo4jServer.close()
    super.afterAll()
  }
}
