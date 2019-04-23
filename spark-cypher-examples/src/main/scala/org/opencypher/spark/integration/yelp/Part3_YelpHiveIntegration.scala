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

import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.opencypher.graphddl.GraphDdl
import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.Jdbc
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._

object Part3_YelpHiveIntegration extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 4 - Hive Integration")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpSubsetFolder)

  implicit val morpheus: CAPSSession = CAPSSession.local()
  implicit val spark: SparkSession = morpheus.sparkSession
  import morpheus._

  val integratedGraphName = GraphName(s"${yelpDB}_and_$yelpBookDB")

  prepareDemoData()

  val h2Config = SqlDataSourceConfig.Jdbc(
    url = s"jdbc:h2:mem:$yelpBookDB.db;INIT=CREATE SCHEMA IF NOT EXISTS $yelpBookDB;DB_CLOSE_DELAY=30;",
    driver = "org.h2.Driver"
  )

  initH2(h2Config)
  initHive()

  val graphDdl =
    s"""
       |CREATE GRAPH $integratedGraphName (
       |  -- Graph schema
       |  Business ( businessId STRING, name STRING, city STRING, state STRING ),
       |  User     ( name STRING ),
       |  REVIEWS  ( stars INTEGER ),
       |  FRIEND,
       |
       |  -- Load Yelp users and businesses from Hive
       |  (User)     FROM HIVE.$yelpDB.user,
       |  (Business) FROM HIVE.$yelpDB.business (business_id AS businessId),
       |
       |  -- Load Yelp reviews from Hive
       |  (User)-[REVIEWS]->(Business) FROM HIVE.$yelpDB.review e
       |    START NODES (User)     FROM HIVE.$yelpDB.user     n JOIN ON e.user_email  = n.email
       |    END   NODES (Business) FROM HIVE.$yelpDB.business n JOIN ON e.business_id = n.business_id,
       |
       |  -- Load YelpBook friendships from H2 (via JDBC) and join with Hive data using email address
       |  (User)-[FRIEND]->(User) FROM H2.$yelpBookDB.friend e
       |    START NODES (User)     FROM HIVE.$yelpDB.user     n JOIN ON e.user1_email = n.email
       |    END   NODES (User)     FROM HIVE.$yelpDB.user     n JOIN ON e.user2_email = n.email
       |)
     """.stripMargin

  // Load integrated graph using SQL Property Graph Data Source using above DDL script and two data sources
  val sqlPgds = GraphSources
    .sql(GraphDdl(graphDdl))
    .withSqlDataSourceConfigs("HIVE" -> SqlDataSourceConfig.Hive, "H2" -> h2Config)

  registerSource(Namespace("federation"), sqlPgds)

  cypher(
    s"""
       |FROM GRAPH federation.$integratedGraphName
       |MATCH (user1:User)-[:REVIEWS]->(b:Business)<-[:REVIEWS]-(user2:User)
       |RETURN EXISTS((user1)-[:FRIEND]-(user2)), count(b) AS coReviews
     """.stripMargin).show

  def initH2(conf: Jdbc): Unit = {
    spark.read
      .json(s"$inputPath/$cityGraphName/$yelpBookDB/friend.json")
      .write
      .format("jdbc")
      .mode("ignore")
      .option("url", conf.url)
      .option("driver", conf.driver)
      .options(conf.options)
      .option("dbtable", s"$yelpBookDB.friend")
      .save
  }

  def initHive(): Unit = {
    import spark._

    sql(s"DROP DATABASE IF EXISTS $yelpDB CASCADE")
    sql(s"CREATE DATABASE $yelpDB")
    sql(s"USE $yelpDB")

    read.json(s"$inputPath/$cityGraphName/$yelpDB/business.json").write.saveAsTable(s"$yelpDB.business")
    read.json(s"$inputPath/$cityGraphName/$yelpDB/user.json").write.saveAsTable(s"$yelpDB.user")
    read.json(s"$inputPath/$cityGraphName/$yelpDB/review.json").write.saveAsTable(s"$yelpDB.review")
  }

  def prepareDemoData(): Unit = if (!Paths.get(inputPath).toFile.exists()) {
    YelpHelpers.extractYelpCitySubset(defaultYelpJsonFolder, inputPath, city)
  }
}
