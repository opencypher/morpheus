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

import org.apache.spark.sql.types.{ArrayType, DateType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.api.io.GraphEntity._
import org.opencypher.spark.api.io.Relationship._
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.impl.table.SparkTable._
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part1_YelpImport extends App {

  lazy val inputPath = args.headOption.getOrElse(defaultYelpJsonFolder)
  lazy val outputPath = args.lift(1).getOrElse(defaultYelpGraphFolder)

  implicit val caps: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = caps.sparkSession

  storeGraph(inputPath, outputPath)

  def storeGraph(inputPath: String, outputPath: String): Unit = {
    val yelpTables = loadYelpTables(inputPath)
    val propertyGraph = createPropertyGraph(yelpTables)
    GraphSources.fs(outputPath).parquet.store(yelpGraphName, propertyGraph)
  }

  def createPropertyGraph(yelpTables: YelpTables): PropertyGraph = {

    val userNodeTable = CAPSEntityTable.create(NodeMappingBuilder.on(sourceIdKey)
      .withImpliedLabel(userLabel)
      .withPropertyKey("name")
      .withPropertyKey("elite")
      .build,
      yelpTables.userDf.prependIdColumn(sourceIdKey, userLabel))

    val businessNodeTable = CAPSEntityTable.create(NodeMappingBuilder.on(sourceIdKey)
      .withImpliedLabel(businessLabel)
      .withPropertyKey("businessId", "business_id")
      .withPropertyKey("name")
      .withPropertyKey("address")
      .withPropertyKey("city")
      .withPropertyKey("state")
      .build,
      yelpTables.businessDf.prependIdColumn(sourceIdKey, businessLabel))

    val reviewRelTable = CAPSEntityTable.create(RelationshipMappingBuilder.on(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(reviewRelType)
      .withPropertyKey("stars")
      .withPropertyKey("date")
      .build,
      yelpTables.reviewDf
        .prependIdColumn(sourceIdKey, reviewRelType)
        .prependIdColumn(sourceStartNodeKey, userLabel)
        .prependIdColumn(sourceEndNodeKey, businessLabel))

    val friendRelTable = CAPSEntityTable.create(RelationshipMappingBuilder.on(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(friendRelType)
      .build,
      yelpTables.friendDf
        .prependIdColumn(sourceIdKey, friendRelType)
        .prependIdColumn(sourceStartNodeKey, userLabel)
        .prependIdColumn(sourceEndNodeKey, userLabel))

    caps.graphs.create(businessNodeTable, userNodeTable, reviewRelTable, friendRelTable)
  }

  def loadYelpTables(inputPath: String)(implicit spark: SparkSession): YelpTables = {
    val rawBusinessDf = spark.read.json(s"$inputPath/business.json")
    val rawReviewDf = spark.read.json(s"$inputPath/review.json")
    val rawUserDf = spark.read.json(s"$inputPath/user.json")

    import spark.implicits._

    val businessDf = rawBusinessDf.select($"business_id".as(sourceIdKey), $"business_id", $"name", $"address", $"city", $"state")
    val reviewDf = rawReviewDf.select($"review_id".as(sourceIdKey), $"user_id".as(sourceStartNodeKey), $"business_id".as(sourceEndNodeKey), $"stars", $"date".cast(DateType))
    val userDf = rawUserDf.select($"user_id".as(sourceIdKey), $"name", functions.split($"elite", ",").cast(ArrayType(LongType)).as("elite"))
    val friendDf = rawUserDf
      .select($"user_id".as(sourceStartNodeKey), functions.explode(functions.split($"friends", ", ")).as(sourceEndNodeKey))
      .withColumn(sourceIdKey, functions.monotonically_increasing_id())

    YelpTables(userDf, businessDf, reviewDf, friendDf)
  }

  implicit class DataFrameOps(df: DataFrame) {
    def prependIdColumn(idColumn: String, prefix: String): DataFrame =
      df.transformColumns(idColumn)(column => functions.concat(functions.lit(prefix), column).as(idColumn))
  }

  case class YelpTables(
    userDf: DataFrame,
    businessDf: DataFrame,
    reviewDf: DataFrame,
    friendDf: DataFrame
  ) {
    def show(): Unit = {
      userDf.show()
      businessDf.show()
      reviewDf.show()
      friendDf.show()
    }
  }
}
