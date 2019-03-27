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

import java.net.URI

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpOptions._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object YelpRecommendationDemo extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = caps.sparkSession

  private val neo4jConfig = Neo4jConfig(new URI("bolt://localhost:7687"), "neo4j", Some("yelp"))
  neo4jConfig.withSession{session =>
    session.run("MATCH (n) DETACH DELETE n").consume()
    session.run("DROP CONSTRAINT ON ( ___yelp:___yelp ) ASSERT ___yelp.___capsID IS UNIQUE").consume()
  }

  Measurement.printTiming("Analytical workflow")(run(inputPath))

  def run(path: String): Unit = {


    // Create Property Graph Data Sources
    val parquetPgds = GraphSources.fs(path).parquet
    val neo4jPgds = GraphSources.cypher.neo4j(neo4jConfig)

    // Load Yelp graph from Parquet (lazy)
    val yelpGraph = parquetPgds.graph(yelpGraphName)

    // Compute Phoenix subgraph containing business and their reviews
    val phoenixGraph = yelpGraph.cypher(
      """
        |MATCH (b:Business)<-[r:REVIEWS]-(user:User)
        |WHERE b.city = 'Phoenix'
        |CONSTRUCT
        | CREATE (user)-[COPY OF r]->(b)
        |RETURN GRAPH
      """.stripMargin).graph

    // Write Phoenix graph to Neo4j
    neo4jPgds.store(yelpGraphName, phoenixGraph)

    // Compute PageRank using Neo4j Graph Algorithms
    val pageRankStats = neo4jConfig.cypherWithNewSession(
      """
        |CALL algo.pageRank(null, null, {
        |  iterations:20,
        |  dampingFactor:0.85,
        |  direction: "BOTH",
        |  write: true,
        |  writeProperty:"pageRank",
        |  weightProperty: "stars"
        |})
        |YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty
      """.stripMargin)


    // Read updated graph from Neo4j
    val phoenixGraphWithPageRank = GraphSources.cypher.neo4j(neo4jConfig).graph(yelpGraphName)

    // Compute top businesses
    val topBusinesses = phoenixGraphWithPageRank.cypher(
      """
        |MATCH (b:Business)
        |RETURN b.name, b.pageRank
        |ORDER BY b.pageRank DESC
        |LIMIT 10
      """.stripMargin)

    topBusinesses.show
    println(pageRankStats)
  }
}
