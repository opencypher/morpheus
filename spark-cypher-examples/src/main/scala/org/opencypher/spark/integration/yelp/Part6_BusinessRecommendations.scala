package org.opencypher.spark.integration.yelp

import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants.{coReviewAndBusinessGraphName, defaultYelpGraphFolder, fsNamespace, log, neo4jConfig, neo4jNamespace, reviewCountProperty, reviewRelType}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.{cypher => neo4jCypher, _}
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._


object Part6_BusinessRecommendations extends App {
  log("Part 6 - Business Recommendation")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  val parquetPGDS = GraphSources.fs(inputPath).parquet
  registerSource(fsNamespace, parquetPGDS)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))


  val year = 2017

  if (!parquetPGDS.hasGraph(coReviewAndBusinessGraphName(year)))
    cypher(
      s"""
         |CATALOG CREATE GRAPH $neo4jNamespace.${coReviewAndBusinessGraphName(year)} {
         |  FROM $fsNamespace.${coReviewAndBusinessGraphName(year)}
         |  RETURN GRAPH
         |}
     """.stripMargin)

  // Compute Louvain using Neo4j Graph Algorithms
  neo4jConfig.withSession { implicit session =>
    log("Find communities via Louvain")
    val communityNumber = neo4jCypher(
      s"""
         |CALL algo.louvain('${coReviewAndBusinessGraphName(year).metaLabel}', 'CO_REVIEWS', {
         |    weightProperty:'$reviewCountProperty',
         |    write:true,
         |    writeProperty:'community_label'})
         |    YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;
    """.stripMargin).head("communityCount").cast[CypherInteger].value.toInt

    log(s"Finding similar users inside the $communityNumber communities")

    (0 until communityNumber).foreach { communityNumber =>
      neo4jCypher(
        s"""
           | MATCH (u:User)-[r:$reviewRelType]-(b:Business)
           | WHERE u.community_label = $communityNumber
           | WITH {item:id(u), categories: collect(id(b))} as userData
           | WITH collect (userData) as data
           | CALL algo.similarity.jaccard(data,{similarityCutoff:0.5, write: true, writeRelationshipType:'JaccardSimilar_2017_Reviews'})
           | YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
           | RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
       """.stripMargin
      )
    }
  }


  log("Find recommendations")

  val recommendations = cypher(
    s"""
       |FROM GRAPH $neo4jNamespace.${coReviewAndBusinessGraphName(year)}
       |MATCH (u:User)-[:JaccardSimilar_2017_Reviews]->(o:User),
       |      (o:User)-[r:$reviewRelType]->(b:Business)
       |WHERE not((u)<--(b:Business)) AND r.stars > 3
       |WITH id(u) as user_id, u.name as name, COLLECT(DISTINCT b.name) as recommendations
       |RETURN name as User, recommendations
       |ORDER BY user_id DESC
       |LIMIT 10
       """.stripMargin
  )

  println(recommendations.show)
}


