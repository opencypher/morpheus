package org.opencypher.spark.integration.yelp

import org.apache.commons.math3.stat.inference.TTest
import org.opencypher.okapi.api.value.CypherValue.CypherFloat
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.{cypher => neo4jCypher, _}
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part4_EliteValidation extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  val (eliteRanks, nonEliteRanks) = (2015 to 2018).map { year =>
    cypher(
      s"""
         |CATALOG CREATE GRAPH $neo4jNamespace.${coReviewGraphName(year)} {
         |  FROM $fsNamespace.${coReviewGraphName(year)}
         |  RETURN GRAPH
         |}
     """.stripMargin)

    // Compute PageRank using Neo4j Graph Algorithms
    neo4jConfig.withSession { implicit session =>
      println(neo4jCypher(
        s"""
           |CALL algo.pageRank('${coReviewGraphName(year).metaLabel}', null, {
           |  iterations:     20,
           |  dampingFactor:  0.85,
           |  direction:      "BOTH",
           |  write:          true,
           |  writeProperty:  "${pageRankCoReviewProp(year)}"
           |})
           |YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty
    """.stripMargin))

      val elitePageRank = neo4jCypher(
        s"""
           |MATCH (u:User)
           |WHERE $year IN u.elite AND exists(u.${pageRankCoReviewProp(year)})
           |RETURN avg(u.${pageRankCoReviewProp(year)}) AS avg
         """.stripMargin).head("avg").cast[CypherFloat].value

      val noneElitePageRank = neo4jCypher(
        s"""
           |MATCH (u:User)
           |WHERE NOT $year IN u.elite AND exists(u.${pageRankCoReviewProp(year)})
           |RETURN avg(u.${pageRankCoReviewProp(year)}) AS avg
         """.stripMargin).head("avg").cast[CypherFloat].value

      elitePageRank -> noneElitePageRank
    }
  }.unzip

  println(eliteRanks)
  println(nonEliteRanks)

  println(s"pValue: ${new TTest().tTest(eliteRanks.toArray, nonEliteRanks.toArray)}")
}
