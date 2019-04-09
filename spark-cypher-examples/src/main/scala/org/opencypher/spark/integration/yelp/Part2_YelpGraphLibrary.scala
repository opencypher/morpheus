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
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants.{defaultYelpGraphFolder, yelpGraphName, _}
import org.opencypher.spark.impl.CAPSConverters._

object Part2_YelpGraphLibrary extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 2 - Create graphs")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()

  import caps._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)

  // Construct City sub graph
  log("Construct city sub graph", 1)
  cypher(
    s"""
       |CATALOG CREATE GRAPH $cityGraphName {
       |  FROM GRAPH $fsNamespace.$yelpGraphName
       |  MATCH (b:Business)<-[r:REVIEWS]-(user1:User)
       |  WHERE b.city = '$city'
       |  CONSTRUCT
       |   CREATE (user1)-[r]->(b)
       |  RETURN GRAPH
       |}
      """.stripMargin)

  // Cache graph before performing multiple projections
  catalog.source(catalog.sessionNamespace).graph(cityGraphName).asCaps.cache()

  // Create multiple projections of the City graph and store them in yearly buckets
  log(s"Create graph projections for '$city'", 1)
  (2015 to 2018) foreach { year =>
    log(s"For year $year", 2)
    // Compute (:User)-[:REVIEWS]->(:Business) graph
    cypher(
      s"""
         |CATALOG CREATE GRAPH $fsNamespace.${reviewGraphName(year)} {
         |  FROM GRAPH $cityGraphName
         |  MATCH (b:Business)<-[r:REVIEWS]-(user:User)
         |  WHERE r.date.year = $year AND user.yelping_since.year <= $year
         |  CONSTRUCT
         |   CREATE (user)-[r]->(b)
         |  RETURN GRAPH
         |}
     """.stripMargin)

    // Compute (:User)-[:CO_REVIEWS]->(:User) graph
    cypher(
      s"""
         |CATALOG CREATE GRAPH $fsNamespace.${coReviewGraphName(year)} {
         |  FROM GRAPH $fsNamespace.${reviewGraphName(year)}
         |  MATCH (b:Business)<-[r1:REVIEWS]-(user1:User),
         |        (b)<-[r2:REVIEWS]-(user2:User)
         |  WHERE r1.date.year = $year
         |    AND r2.date.year = $year
         |    AND user1.yelping_since.year <= $year AND user2.yelping_since.year <= $year
         |  WITH user1, user2, count(b) AS reviewCount
         |  CONSTRUCT
         |    CREATE (user1)-[r:CO_REVIEWS { $reviewCountProperty : $reviewCountProperty }]->(user2)
         |  RETURN GRAPH
         |}
     """.stripMargin)

    // Compute (u1:User)-[:CO_REVIEWS]->(u2:User)-[:REVIEWS]->(:Business)<-[:REVIEWS]-(u1) graph
    cypher(
      s"""
         |CATALOG CREATE GRAPH $fsNamespace.${coReviewAndBusinessGraphName(year)} {
         |  FROM GRAPH $fsNamespace.${reviewGraphName(year)}
         |  MATCH (b:Business)<-[r1:REVIEWS]-(user1:User),
         |        (b)<-[r2:REVIEWS]-(user2:User)
         |  WHERE r1.date.year = $year
         |    AND r2.date.year = $year
         |    AND user1.yelping_since.year <= $year AND user2.yelping_since.year <= $year
         |  WITH b, user1, r1, user2, r2, count(b) AS $reviewCountProperty
         |  CONSTRUCT
         |    CREATE (user1)-[r1]->(b)
         |    CREATE (user2)-[r2]->(b)
         |    CREATE (user1)-[r:CO_REVIEWS { $reviewCountProperty : $reviewCountProperty }]->(user2)
         |  RETURN GRAPH
         |}
     """.stripMargin)
  }
}
