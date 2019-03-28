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
