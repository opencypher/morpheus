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

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.okapi.neo4j.io.Neo4jConfig

object YelpConstants {

  val neo4jConfig = Neo4jConfig(new URI("bolt://localhost:7687"), "neo4j", Some("yelp"))

  val yelpGraphName = GraphName("yelp")

  val defaultYelpJsonFolder = "yelp_json"
  val defaultYelpGraphFolder = "yelp_graph"

  val userLabel = "User"
  val businessLabel = "Business"
  val reviewRelType = "REVIEWS"
  val friendRelType = "FRIEND"

  val fsNamespace = Namespace("fileSystem")
  val neo4jNamespace = Namespace("neo4j")
  val hiveNamespace = Namespace("hive")

  val phoenixGraphName = GraphName("phoenix")
  val businessTrendsGraphName = GraphName("businessTrends")

  val view2017: String = yearGraphName(2017).value
  val view2018: String = yearGraphName(2018).value

  def yearGraphName(year: Int) = GraphName(s"year$year")
  def coReviewGraphName(year: Int) = GraphName(s"coReviewYear$year")

  def pageRankProp(year: Int) = s"pageRank$year"
  def pageRankCoReviewProp(year: Int) = s"pageRankCoReview$year"

}
