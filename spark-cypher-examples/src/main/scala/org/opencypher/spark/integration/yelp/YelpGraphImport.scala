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

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.api.{CAPSSession, GraphSources}

object YelpGraphImport extends App {

  case class YelpTables(
    userDf: DataFrame,
    businessDf: DataFrame,
    userReviewsBusinessDf: DataFrame,
    businessReviewedByUserDf: DataFrame,
    friendDf: DataFrame
  )

  lazy val inputPath = args.headOption.getOrElse("yelp_json")
  lazy val outputPath = args.lift(1).getOrElse("yelp_graph")

  implicit val caps: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = caps.sparkSession

  storeGraph(inputPath, outputPath)

  def storeGraph(inputPath: String, outputPath: String): Unit = {
    val yelpTables = loadYelpTables(inputPath)
    val propertyGraph = createPropertyGraph(yelpTables)
    GraphSources.fs(outputPath).parquet.store(GraphName("yelp"), propertyGraph)
  }

  def createPropertyGraph(yelpTables: YelpTables): PropertyGraph = {

    val userNodeTable = CAPSEntityTable.create(NodeMappingBuilder.on("id")
      .withImpliedLabel("User")
      .withPropertyKey("name")
      .build, yelpTables.userDf)

    val businessNodeTable = CAPSEntityTable.create(NodeMappingBuilder.on("id")
      .withImpliedLabel("Business")
      .withPropertyKey("name")
      .withPropertyKey("city")
      .withPropertyKey("state")
      .build, yelpTables.businessDf)

    val userReviewsBusinessRelTable = CAPSEntityTable.create(RelationshipMappingBuilder.on("id")
      .withSourceStartNodeKey("source_id")
      .withSourceEndNodeKey("target_id")
      .withRelType("REVIEWS")
      .withPropertyKey("stars")
      .build, yelpTables.userReviewsBusinessDf)

    val businessReviewedByUserRelTable = CAPSEntityTable.create(RelationshipMappingBuilder.on("id")
      .withSourceStartNodeKey("source_id")
      .withSourceEndNodeKey("target_id")
      .withRelType("REVIEWED_BY")
      .withPropertyKey("stars")
      .build, yelpTables.businessReviewedByUserDf)

    val friendsRelTable = CAPSEntityTable.create(RelationshipMappingBuilder.on("id")
      .withSourceStartNodeKey("source_id")
      .withSourceEndNodeKey("target_id")
      .withRelType("FRIEND")
      .build, yelpTables.friendDf)

    caps.graphs.create(businessNodeTable, userNodeTable, userReviewsBusinessRelTable, businessReviewedByUserRelTable, friendsRelTable)
  }

  def loadYelpTables(inputPath: String): YelpTables = {
    val rawBusinessDf = spark.read.json(s"$inputPath/business.json")
    val rawReviewDf = spark.read.json(s"$inputPath/review.json")
    val rawUserDf = spark.read.json(s"$inputPath/user.json")

    import spark.implicits._

    val businessDf = rawBusinessDf.select($"business_id".as("id"), $"name", $"city", $"state")
    val userReviewsBusinessDf = rawReviewDf.select($"review_id".as("id"), $"user_id".as("source_id"), $"business_id".as("target_id"), $"stars")
    val businessReviewedByUserDf = rawReviewDf.select($"review_id".as("id"), $"business_id".as("source_id"), $"user_id".as("target_id"), $"stars")
    val userDf = rawUserDf.select($"user_id".as("id"), $"name".as("name"))
    val friendDf = rawUserDf
      .select($"user_id".as("source_id"), functions.explode(functions.split($"friends", ", ")).as("target_id"))
      .withColumn("id", functions.monotonically_increasing_id())

    YelpTables(userDf, businessDf, userReviewsBusinessDf, businessReviewedByUserDf, friendDf)
  }
}
