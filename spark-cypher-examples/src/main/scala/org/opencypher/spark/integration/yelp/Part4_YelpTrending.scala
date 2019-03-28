package org.opencypher.spark.integration.yelp

import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part4_YelpTrending extends App {

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  cypher(
    s"""
       |CATALOG CREATE GRAPH $businessTrendsGraphName {
       |  FROM GRAPH $neo4jNamespace.$pre2017GraphName
       |  MATCH (b1:Business)
       |  FROM GRAPH $neo4jNamespace.$since2017GraphName
       |  MATCH (b2:Business)
       |  WHERE b1.businessId = b2.businessId
       |  WITH b1 AS b, b2.$pageRankSince2017 - b1.$pageRankPre2017 AS trendRank
       |  CONSTRUCT
       |    CREATE (newB COPY OF b)
       |    SET newB.trendRank = trendRank
       |  RETURN GRAPH
       |}
     """.stripMargin)

  cypher(s"FROM GRAPH $businessTrendsGraphName MATCH (n) RETURN n LIMIT 10").show

  // Increasing popularity
  cypher(
    s"""
       |FROM GRAPH $businessTrendsGraphName
       |MATCH (b:Business)
       |RETURN b.name AS name, b.address AS address, b.trendRank AS trendRank
       |ORDER BY trendRank DESC
       |LIMIT 10
     """.stripMargin).show

  // Decreasing popularity
  cypher(
    s"""
       |FROM GRAPH $businessTrendsGraphName
       |MATCH (b:Business)
       |RETURN b.name AS name, b.address AS address, b.trendRank AS trendRank
       |ORDER BY trendRank ASC
       |LIMIT 10
     """.stripMargin).show
}
