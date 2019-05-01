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
package org.opencypher.morpheus.integration.yelp

import org.apache.log4j.{Level, Logger}
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.integration.yelp.YelpConstants._
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.{cypher => neo4jCypher, _}

object Part5_BusinessRecommendations extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 5 - Business Recommendation")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val morpheus: MorpheusSession = MorpheusSession.local()
  import morpheus._

  registerSource(fsNamespace, GraphSources.fs(inputPath).parquet)
  registerSource(neo4jNamespace, GraphSources.cypher.neo4j(neo4jConfig))

  val year = 2017

  log("Write to Neo4j, detect communities and find similar users within communities", 1)
  cypher(
    s"""
       |CATALOG CREATE GRAPH $neo4jNamespace.${coReviewAndBusinessGraphName(year)} {
       |  FROM $fsNamespace.${coReviewAndBusinessGraphName(year)}
       |  RETURN GRAPH
       |}
     """.stripMargin)

  // Use Neo4j Graph Algorithms to compute Louvain clusters and Jaccard similarity within clusters
  neo4jConfig.withSession { implicit session =>

    log("Find communities via Louvain", 1)
    val louvainStats = neo4jCypher(
      s"""
         |CALL algo.louvain('${coReviewAndBusinessGraphName(year).metaLabel}', 'CO_REVIEWS', {
         |  write:           true,
         |  weightProperty: 'reviewCount',
         |  writeProperty:  '${communityProp(year)}'
         |})
         |YIELD communityCount, nodes, loadMillis, computeMillis, writeMillis
         |RETURN communityCount, nodes, loadMillis + computeMillis + writeMillis AS total""".stripMargin).head
    log(s"Computing Louvain modularity on ${louvainStats("nodes")} nodes took ${louvainStats("total")} ms", 1)

    val communityNumber = louvainStats("communityCount").cast[CypherInteger].value.toInt

    log(s"Find similar users within $communityNumber communities", 1)
    // We use Jaccard similarity because it doesn't require equal length vectors
    (0 until communityNumber).foreach { communityNumber =>
      neo4jCypher(
        s"""
           |MATCH (u:User)-[r:REVIEWS]->(b:Business)
           |WHERE u.${communityProp(year)} = $communityNumber
           |WITH { item: id(u), categories: collect(id(b))} AS userData
           |WITH collect(userData) AS data
           |CALL algo.similarity.jaccard(data, {
           |  similarityCutoff:      0.5,
           |  write:                 true,
           |  writeRelationshipType: '${isSimilarRelType(year)}'})
           |YIELD similarityPairs
           |RETURN similarityPairs
       """.stripMargin
      )
    }
  }

  log("Load graphs back to Spark and compute recommendations", 1)

  // Reset schema cache to enable loading new properties
  catalog.source(neo4jNamespace).reset()

  val recommendations = cypher(
    s"""
       |FROM GRAPH $neo4jNamespace.${coReviewAndBusinessGraphName(year)}
       |MATCH (u:User)-[:${isSimilarRelType(year)}]-(o:User),
       |      (o:User)-[r:REVIEWS]->(b:Business)
       |WHERE NOT((u)<-[:REVIEWS]-(b:Business)) AND r.stars > 3
       |WITH id(u) AS user_id, u.name AS name, collect(DISTINCT b.name) AS recommendations
       |RETURN name AS user, recommendations
       |ORDER BY user_id DESC
       |LIMIT 10
       """.stripMargin
  )

  recommendations.show
}


