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

import org.apache.commons.math3.stat.inference.TTest
import org.apache.log4j.{Level, Logger}
import org.opencypher.okapi.api.value.CypherValue.CypherFloat
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.{cypher => neo4jCypher, _}
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part3b_EliteValidation extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 3b - Elite validation")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()
  import caps._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  log("Write to Neo4j and compute pageRank", 1)
  val (eliteRanks, nonEliteRanks) = (2015 to 2018).map { year =>
    log(s"For year $year", 2)
    cypher(
      s"""
         |CATALOG CREATE GRAPH $neo4jNamespace.${coReviewsGraphName(year)} {
         |  FROM $fsNamespace.${coReviewsGraphName(year)}
         |  RETURN GRAPH
         |}
     """.stripMargin)

    // Compute PageRank using Neo4j Graph Algorithms
    neo4jConfig.withSession { implicit session =>
      val pageRankStats = neo4jCypher(
        s"""
           |CALL algo.pageRank('${coReviewsGraphName(year).metaLabel}', null, {
           |  iterations:     20,
           |  dampingFactor:  0.85,
           |  direction:      "BOTH",
           |  write:          true,
           |  writeProperty:  "${pageRankCoReviewProp(year)}",
           |  weightProperty: "reviewCount"
           |})
           |YIELD nodes, loadMillis, computeMillis, writeMillis
           |RETURN nodes, loadMillis + computeMillis + writeMillis AS total""".stripMargin).head
      log(s"Computing page rank on ${pageRankStats("nodes")} nodes took ${pageRankStats("total")} ms", 2)

      val elitePageRank = neo4jCypher(
        s"""
           |MATCH (u:User)
           |WHERE $year IN u.elite AND exists(u.${pageRankCoReviewProp(year)})
           |RETURN avg(u.${pageRankCoReviewProp(year)}) AS avg
         """.stripMargin).head("avg").cast[CypherFloat].value
      log(s"Average elite page rank: $elitePageRank", 2)

      val noneElitePageRank = neo4jCypher(
        s"""
           |MATCH (u:User)
           |WHERE u.elite IS NULL OR NOT $year IN u.elite AND exists(u.${pageRankCoReviewProp(year)})
           |RETURN avg(u.${pageRankCoReviewProp(year)}) AS avg
         """.stripMargin).head("avg").cast[CypherFloat].value
      log(s"Average non-elite page rank: $noneElitePageRank", 2)

      elitePageRank -> noneElitePageRank
    }
  }.unzip

  println(s"pValue: ${new TTest().tTest(eliteRanks.toArray, nonEliteRanks.toArray)}")
}
