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

import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.opencypher.morpheus.api.io.GraphElement.sourceIdKey
import org.opencypher.morpheus.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.morpheus.impl.table.SparkTable._
import org.opencypher.morpheus.integration.yelp.YelpConstants._

object YelpHelpers {

  case class YelpTables(
    userDf: DataFrame,
    businessDf: DataFrame,
    reviewDf: DataFrame
  )

  def loadYelpTables(inputPath: String)(implicit spark: SparkSession): YelpTables = {
    import spark.implicits._

    log("read business.json", 2)
    val rawBusinessDf = spark.read.json(s"$inputPath/business.json")
    log("read review.json", 2)
    val rawReviewDf = spark.read.json(s"$inputPath/review.json")
    log("read user.json", 2)
    val rawUserDf = spark.read.json(s"$inputPath/user.json")

    val businessDf = rawBusinessDf.select($"business_id".as(sourceIdKey), $"business_id", $"name", $"address", $"city", $"state")
    val reviewDf = rawReviewDf.select($"review_id".as(sourceIdKey), $"user_id".as(sourceStartNodeKey), $"business_id".as(sourceEndNodeKey), $"stars", $"date".cast(DateType))
    val userDf = rawUserDf.select(
      $"user_id".as(sourceIdKey),
      $"name",
      $"yelping_since".cast(DateType),
      functions.split($"elite", ",").cast(ArrayType(LongType)).as("elite"))

    YelpTables(userDf, businessDf, reviewDf)
  }

  def printYelpStats(inputPath: String)(implicit spark: SparkSession): Unit = {
    val rawBusinessDf = spark.read.json(s"$inputPath/business.json")
    val rawReviewDf = spark.read.json(s"$inputPath/review.json")

    import spark.implicits._

    rawBusinessDf.select($"city", $"state").distinct().show()
    rawBusinessDf.withColumnRenamed("business_id", "id")
      .join(rawReviewDf, $"id" === $"business_id")
      .groupBy($"city", $"state")
      .count().as("count")
      .orderBy($"count".desc, $"state".asc)
      .show(100)
  }

  def extractYelpCitySubset(inputPath: String, outputPath: String, city: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    def emailColumn(userId: String): Column = functions.concat($"$userId", functions.lit("@yelp.com"))

    val rawUserDf = spark.read.json(s"$inputPath/user.json")
    val rawReviewDf = spark.read.json(s"$inputPath/review.json")
    val rawBusinessDf = spark.read.json(s"$inputPath/business.json")

    val businessDf = rawBusinessDf.filter($"city" === city)
    val reviewDf = rawReviewDf
      .join(businessDf, Seq("business_id"), "left_semi")
      .withColumn("user_email", emailColumn("user_id"))
      .withColumnRenamed("stars", "stars_tmp")
      .withColumn("stars", $"stars_tmp".cast(IntegerType))
      .drop("stars_tmp")
    val userDf = rawUserDf
      .join(reviewDf, Seq("user_id"), "left_semi")
      .withColumn("email", emailColumn("user_id"))
    val friendDf = userDf
      .select($"email".as("user1_email"), functions.explode(functions.split($"friends", ", ")).as("user2_id"))
      .withColumn("user2_email", emailColumn("user2_id"))
      .select(s"user1_email", s"user2_email")

    businessDf.write.json(s"$outputPath/$cityGraphName/$yelpDB/business.json")
    reviewDf.write.json(s"$outputPath/$cityGraphName/$yelpDB/review.json")
    userDf.write.json(s"$outputPath/$cityGraphName/$yelpDB/user.json")
    friendDf.write.json(s"$outputPath/$cityGraphName/$yelpBookDB/friend.json")
  }

  implicit class DataFrameOps(df: DataFrame) {
    def prependIdColumn(idColumn: String, prefix: String): DataFrame =
      df.transformColumns(idColumn)(column => functions.concat(functions.lit(prefix), column).as(idColumn))
  }
}
