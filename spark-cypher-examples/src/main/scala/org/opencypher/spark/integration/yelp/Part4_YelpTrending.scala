package org.opencypher.spark.integration.yelp

import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part4_YelpTrending extends App {

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  cypher(
    s"""
       |FROM GRAPH $neo4jNamespace.$pre2017GraphName
       |MATCH (b1:Business)
       |FROM GRAPH $neo4jNamespace.$since2017GraphName
       |MATCH (b2:Business)
       |WHERE b1.businessId = b2.businessId
       |RETURN b1.name AS name, b1.address AS address, b2.$pageRankSince2017 - b1.$pageRankPre2017 AS trendRank
       |ORDER BY trendRank DESC
       |LIMIT 10
     """.stripMargin).show
}
