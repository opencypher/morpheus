package org.opencypher.spark.api.io.neo4j.sync

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport
import org.opencypher.spark.api.GraphSources
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.CAPSNeo4jServerFixture

class Neo4jGraphMergeBugTest extends CAPSTestSuite with CAPSNeo4jServerFixture {
  override def dataFixture: String = ""

  override def beforeAll(): Unit = {
    super.beforeAll()
    caps.registerSource(Namespace("neo4j"),
      GraphSources.cypher.neo4j(neo4jConfig)
    )
    caps.registerSource(Namespace("csv"),
      GraphSources.fs(getClass.getResource("/bug").getPath).csv
    )
  }

  test("should get same query results after merging") {

    val graph = caps.catalog.graph("csv.buggy")

    val entityKeys = EntityKeys(
      Map(
        "Interaction" -> Set("interactionId"),
        "Customer" -> Set("customerId"),
        "CustomerRep" -> Set("empNo"),
        "AccountHolder" -> Set("accountHolderId"),
        "Policy" -> Set("policyAccountNumber")
      )
    )

    Neo4jGraphMerge.merge(graph, neo4jConfig, entityKeys)

    val csvResult = caps.cypher(
      """
        |FROM GRAPH csv.buggy
        |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
        |WITH rep, i.type AS type, count(*) AS cnt
        |WHERE type IN ['cancel', 'complaint'] AND cnt > 5
        |RETURN rep, type, cnt
        |ORDER BY type, cnt DESC
      """.stripMargin)

    val neo4jResult = caps.cypher(
      s"""
        |FROM GRAPH neo4j.${MetaLabelSupport.entireGraphName}
        |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
        |WITH rep, i.type AS type, count(*) AS cnt
        |WHERE type IN ['cancel', 'complaint'] AND cnt > 5
        |RETURN rep, type, cnt
        |ORDER BY type, cnt DESC
      """.stripMargin)

    csvResult.show
    neo4jResult.show
  }
}
