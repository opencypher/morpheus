package org.opencypher.spark.examples

import org.neo4j.graphdb.Result
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
      server.close()
    }

    def execute(cypher: String): Result =
      server.graph().execute(cypher)
  }

  def startNeo4j(dataFixture: String): ServerControls = {
    TestServerBuilders
      .newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture("CALL dbms.security.createUser('anonymous', 'password', false)")
      .withFixture(dataFixture)
      .newServer()
  }
}
