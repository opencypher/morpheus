/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.integration.yelp

import org.apache.log4j.{Level, Logger}
import org.opencypher.okapi.api.value.CypherValue.CypherFloat
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.{cypher => neo4jCypher, _}
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part4_BusinessTrends extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 4 - Business trends")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val morpheus: CAPSSession = CAPSSession.local()
  import morpheus._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  log("Write to Neo4j and compute pageRank", 1)
  (2017 to 2018) foreach { year =>
    log(s"For year $year", 2)
    cypher(
      s"""
         |CATALOG CREATE GRAPH $neo4jNamespace.${coReviewedGraphName(year)} {
         |  FROM $fsNamespace.${coReviewedGraphName(year)}
         |  RETURN GRAPH
         |}
     """.stripMargin)

    // Compute PageRank using Neo4j Graph Algorithms
    neo4jConfig.withSession { implicit session =>
      val pageRankStats = neo4jCypher(
        s"""
           |CALL algo.pageRank('${coReviewedGraphName(year).metaLabel}', null, {
           |  iterations:     20,
           |  dampingFactor:  0.85,
           |  direction:      "BOTH",
           |  write:          true,
           |  writeProperty:  "pageRank$year",
           |  weightProperty: "reviewCount"
           |})
           |YIELD nodes, loadMillis, computeMillis, writeMillis
           |RETURN nodes, loadMillis + computeMillis + writeMillis AS total""".stripMargin).head
      log(s"Computing page rank on ${pageRankStats("nodes")} nodes took ${pageRankStats("total")} ms", 2)
    }
  }

  // Reset schema cache to enable loading new properties
  catalog.source(neo4jNamespace).reset()

  // Load graphs from Neo4j into Spark and compute trend rank for each business based on their page ranks.
  log("Load graphs back to Spark and compute trend rank", 1)
  cypher(
    s"""
       |CATALOG CREATE GRAPH $businessTrendsGraphName {
       |  FROM GRAPH $neo4jNamespace.${coReviewedGraphName(2017)}
       |  MATCH (b1:Business)
       |  FROM GRAPH $neo4jNamespace.${coReviewedGraphName(2018)}
       |  MATCH (b2:Business)
       |  WHERE b1.businessId = b2.businessId
       |  WITH b1 AS b, (b2.${pageRankProp(2018)} / ${normalizationFactor(2018)}) - (b1.${pageRankProp(2017)} / ${normalizationFactor(2017)}) AS trendRank
       |  CONSTRUCT
       |    CREATE (newB COPY OF b)
       |    SET newB.trendRank = trendRank
       |  RETURN GRAPH
       |}
     """.stripMargin)

  // Top 10 Increasing popularity
  cypher(
    s"""
       |FROM GRAPH $businessTrendsGraphName
       |MATCH (b:Business)
       |RETURN b.name AS name, b.address AS address, b.trendRank AS trendRank
       |ORDER BY trendRank DESC
       |LIMIT 10
     """.stripMargin).show

  def normalizationFactor(year: Int): Double = neo4jConfig.cypherWithNewSession(
    s"""
       |MATCH (b:Business)
       |RETURN sum(b.${pageRankProp(year)}) AS nf
     """.stripMargin).head("nf").cast[CypherFloat].value
}
