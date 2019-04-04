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

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.Hive
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants.{defaultYelpJsonFolder, _}

object Part5_YelpHive extends App {

  log("Part 5 - Hive")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpJsonFolder)

  implicit val caps: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = caps.sparkSession

  populateHiveTables()

  import caps._

  registerSource(hiveNamespace, GraphSources
    .sql(getClass.getResource("/yelp/ddl/yelp.ddl").getFile)
    .withSqlDataSourceConfigs("hive" -> Hive))

    cypher(
      s"""
         |FROM $hiveNamespace.review2017
         |MATCH ()-[r]->()
         |RETURN COUNT(r)
       """.stripMargin).show

    cypher(
      s"""
         |FROM $hiveNamespace.review2018
         |MATCH ()-[r]->()
         |RETURN COUNT(r)
       """.stripMargin).show

  def populateHiveTables(): Unit = {
    // Create Hive tables from Yelp data
    val yelpTables = Part1_YelpImport.loadYelpTables(inputPath)
    spark.sql(s"DROP DATABASE IF EXISTS $yelpGraphName CASCADE")
    spark.sql(s"CREATE DATABASE $yelpGraphName")
    yelpTables.businessDf.write.saveAsTable(s"$yelpGraphName.business")
    yelpTables.userDf.write.saveAsTable(s"$yelpGraphName.user")
    yelpTables.reviewDf.write.saveAsTable(s"$yelpGraphName.review")
    // Create Hive views
    spark.sql(
      s"""
         |CREATE VIEW $yelpGraphName.reviews2017 AS
         |SELECT *
         |FROM $yelpGraphName.review
         |WHERE year(date) = 2017
     """.stripMargin)

    spark.sql(
      s"""
         |CREATE VIEW $yelpGraphName.reviews2018 AS
         |SELECT *
         |FROM $yelpGraphName.review
         |WHERE year(date) = 2018
     """.stripMargin)
  }
}
