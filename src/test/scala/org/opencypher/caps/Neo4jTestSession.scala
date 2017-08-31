package org.opencypher.caps

import org.apache.spark.sql.Row
import org.neo4j.driver.v1.Config
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.opencypher.caps.api.io.neo4j.EncryptedNeo4jConfig
import org.scalatest.{BeforeAndAfterAll, FunSuite}

object Neo4jTestSession {

  trait Fixture extends BeforeAndAfterAll {

    self: SparkTestSession.Fixture with FunSuite =>

    var neo4jServer: ServerControls = _

    def neo4jConfig = new EncryptedNeo4jConfig(neo4jServer.boltURI(),
      user = "anonymous",
      password = Some("password"),
      encryptionLevel = Config.EncryptionLevel.NONE)

    def neo4jHost: String = {
      val scheme = neo4jServer.boltURI().getScheme
      val userInfo = s"${neo4jConfig.user}:${neo4jConfig.password.get}@"
      val host = neo4jServer.boltURI().getAuthority
      s"$scheme://$userInfo$host"
    }

    private val userFixture = "CALL dbms.security.createUser('anonymous', 'password', false)"

    private val testGraphFixtures =
      """
         CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
         CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
         CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
         CREATE (d:Person:German {name: "Max", luckyNumber: 8})
         CREATE (a)-[:KNOWS {since: 2016}]->(b)
         CREATE (b)-[:KNOWS {since: 2016}]->(c)
         CREATE (c)-[:KNOWS {since: 2016}]->(d)
      """

    override def beforeAll: Unit =
      neo4jServer = TestServerBuilders.newInProcessBuilder()
        .withConfig("dbms.security.auth_enabled", "true")
        .withFixture(userFixture)
        .withFixture(testGraphFixtures)
        .newServer()

    override def afterAll: Unit = neo4jServer.close()

    def testGraphNodes: Set[Row] = Set(
      Row(0, true, true, false, 42, "Stefan"),
      Row(1, true, false, true, 23, "Mats"),
      Row(3, true, true, false, 8, "Max"),
      Row(2, true, true, false, 1337, "Martin")
    )

    def testGraphRels: Set[Row] = Set(
      Row(0, 0, 0, 1, 2016),
      Row(1, 1, 0, 2, 2016),
      Row(2, 2, 0, 3, 2016)
    )
  }
}
