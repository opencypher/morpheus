package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.testing.api.neo4j.Neo4jHarnessUtils.{startNeo4j, _}
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates creating a view on top of an existing Neo4j database.
  */
object Neo4jViewExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Start a Neo4j instance and populate it with social network data
  val neo4j = startNeo4j()

  // Register Property Graph Data Sources (PGDS)
  session.registerSource(Namespace("neo"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))
  session.registerSource(Namespace("fs"), GraphSources.fs(rootPath = getClass.getResource("/csv").getFile).csv)

  // Copy the products from filesystem (csv) to Neo4j
  session.cypher(
    """
      |CREATE GRAPH neo.products {
      | FROM GRAPH fs.products
      | RETURN GRAPH
      |}
    """.stripMargin)

  // Create a view on top of the Neo4j graph that just contains DVDs and customers who bought them
  val viewPGDS = GraphSources.cypher.neo4jView(
    """
      |MATCH (p:Product)
      |WHERE p.category = 'DVD'
      |OPTIONAL MATCH (p)<-[b:BOUGHT]-(c:Customer)
      |CONSTRUCT
      |  CLONE p, b, c
      |  NEW (p)<-[b]-(c)
      |RETURN GRAPH
    """.stripMargin, neo4j.dataSourceConfig)

  session.registerSource(Namespace("DVD_view"), viewPGDS)

  // Return all nodes within the DVD view
  session.cypher(
    """
      |FROM GRAPH DVD_view.products
      |MATCH (n) RETURN n
      |ORDER BY labels(n), n.name, n.title
    """.stripMargin).show

  neo4j.stop()

}
