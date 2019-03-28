package org.opencypher.spark.integration.yelp

import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.spark.integration.yelp.YelpConstants._
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._

object Part3_YelpRanking extends App {

  // Compute PageRank using Neo4j Graph Algorithms
  neo4jConfig.withSession { implicit session =>
    println(cypher(
      s"""
         |CALL algo.pageRank('${pre2017GraphName.metaLabel}', null, {
         |  iterations:20,
         |  dampingFactor:0.85,
         |  direction: "BOTH",
         |  write: true,
         |  writeProperty:"$pageRankPre2017",
         |  weightProperty: "stars"
         |})
         |YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty
    """.stripMargin))

   println(cypher(
      s"""
         |CALL algo.pageRank('${since2017GraphName.metaLabel}', null, {
         |  iterations:20,
         |  dampingFactor:0.85,
         |  direction: "BOTH",
         |  write: true,
         |  writeProperty:"$pageRankSince2017",
         |  weightProperty: "stars"
         |})
         |YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty
    """.stripMargin))
  }
}
