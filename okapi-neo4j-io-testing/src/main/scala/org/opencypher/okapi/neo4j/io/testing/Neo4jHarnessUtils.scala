package org.opencypher.okapi.neo4j.io.testing

import org.neo4j.graphdb.Result
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.opencypher.okapi.neo4j.io.Neo4jConfig

object Neo4jHarnessUtils {

  implicit class RichServerControls(val neo4j: ServerControls) extends AnyVal {

    def dataSourceConfig =
      Neo4jConfig(neo4j.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)

    def uri: String = {
      val scheme = neo4j.boltURI().getScheme
      val userInfo = s"anonymous:password@"
      val host = neo4j.boltURI().getAuthority
      s"$scheme://$userInfo$host"
    }

    def stop(): Unit = {
      neo4j.close()
    }

    def execute(cypher: String): Result =
      neo4j.graph().execute(cypher)

    def withProcedure(procedures: Class[_]*): ServerControls = {
      val proceduresService = neo4j.graph()
        .asInstanceOf[GraphDatabaseAPI]
        .getDependencyResolver.
        resolveDependency(classOf[Procedures])

      for (procedure <- procedures) {
        proceduresService.registerProcedure(procedure, true)
        proceduresService.registerFunction(procedure, true)
        proceduresService.registerAggregationFunction(procedure, true)
      }
      neo4j
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
}
