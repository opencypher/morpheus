package org.opencypher.spark.examples

import org.neo4j.driver.v1.{AuthTokens, Session}
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

object Neo4jHelpers {

  implicit class RichServerControls(val server: ServerControls) extends AnyVal {

    def dataSourceConfig =
      Neo4jConfig(server.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)

    def uri: String = {
      val scheme = server.boltURI().getScheme
      val userInfo = s"anonymous:password@"
      val host = server.boltURI().getAuthority
      s"$scheme://$userInfo$host"
    }

    def stop(): Unit = {
      server.
      server.close()
    }
  }

  def startNeo4j(dataFixture: String): ServerControls = {
    TestServerBuilders
      .newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture("CALL dbms.security.createUser('anonymous', 'password', false)")
      .withFixture(dataFixture)
      .newServer()
  }

  def withBoltSession[T](f: Session => T)(implicit neo4jConfig: Neo4jConfig): T = {
    val driver = org.neo4j.driver.v1.GraphDatabase.driver(
      neo4jConfig.uri, AuthTokens.basic(neo4jConfig.user, neo4jConfig.password.get), neo4jConfig.boltConfig())
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }
}
