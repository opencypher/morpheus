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
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpOptions._

object YelpRecommendationDemo extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = caps.sparkSession

  run(inputPath)

  def run(path: String): Unit = {

    val yelpGraph = GraphSources.fs(path).parquet.graph(yelpGraphName)

    // Phoenix:   (18766 rows)
    // Las Vegas: (29370 rows)
    //    yelpGraph.cypher(
    //      """
    //        |MATCH (b:Business)
    //        |WHERE b.city = 'Phoenix'
    //        |RETURN b.name, b.city, b.state
    //      """.stripMargin).show

    val phoenixGraph = yelpGraph.cypher(
      """
        |MATCH (b:Business)<-[r:REVIEWS]-(user:User)
        |WHERE b.city = 'Phoenix'
        |CONSTRUCT
        | CREATE (b)-[:REVIEWED_BY]->(user)
        | CREATE (user)-[COPY OF r]->(b)
        |RETURN GRAPH
      """.stripMargin).graph

    GraphSources
      .cypher
      .neo4j(Neo4jConfig(new URI("bolt://localhost:7687"), "neo4j", Some("yelp")))
      .store(yelpGraphName, phoenixGraph)
  }
}
