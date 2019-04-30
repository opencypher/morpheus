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
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.spark.api.io.MorpheusElementTable
import org.opencypher.spark.api.io.GraphElement._
import org.opencypher.spark.api.io.Relationship._
import org.opencypher.spark.api.{MorpheusSession, GraphSources}
import org.opencypher.spark.integration.yelp.YelpConstants._
import org.opencypher.spark.integration.yelp.YelpHelpers._

object Part1_YelpImport extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)

  log("Part 1 - Import")

  lazy val inputPath = args.headOption.getOrElse(defaultYelpJsonFolder)
  lazy val outputPath = args.lift(1).getOrElse(defaultYelpGraphFolder)

  implicit val morpheus: MorpheusSession = MorpheusSession.local()
  implicit val spark: SparkSession = morpheus.sparkSession

  storeGraph(inputPath, outputPath)

  def storeGraph(inputPath: String, outputPath: String): Unit = {
    // Load Yelp data into DataFrames
    log("Load yelp tables", 1)
    val yelpTables = loadYelpTables(inputPath)
    // Create a Property Graph from DataFrames
    log("Create property graph", 1)
    val propertyGraph = createPropertyGraph(yelpTables)
    log("Store in parquet", 1)
    storeAsParquet(yelpGraphName, propertyGraph)
  }

  def storeAsParquet(graphName: GraphName, graph: PropertyGraph): Unit = {
    // Init Property Graph Data Source (PGDS)
    val parquetPGDS = GraphSources.fs(outputPath).parquet
    // Store graph in PGDS
    if (parquetPGDS.hasGraph(graphName)) {
      log(s"Warning: A graph with GraphName $graphName already exists.")
    } else {
      parquetPGDS.store(yelpGraphName, graph)
    }
  }

  def createPropertyGraph(yelpTables: YelpTables): PropertyGraph = {
    // Define node tables
    // (:User)
    val userNodeTable = MorpheusElementTable.create(NodeMappingBuilder.on(sourceIdKey)
      .withImpliedLabel(userLabel)
      .withPropertyKey("name")
      .withPropertyKey("yelping_since")
      .withPropertyKey("elite")
      .build,
      yelpTables.userDf.prependIdColumn(sourceIdKey, userLabel))

    // (:Business)
    val businessNodeTable = MorpheusElementTable.create(NodeMappingBuilder.on(sourceIdKey)
      .withImpliedLabel(businessLabel)
      .withPropertyKey("businessId", "business_id")
      .withPropertyKey("name")
      .withPropertyKey("address")
      .withPropertyKey("city")
      .withPropertyKey("state")
      .build,
      yelpTables.businessDf.prependIdColumn(sourceIdKey, businessLabel))

    // Define relationship tables
    // (:User)-[:REVIEWS]->(:Business)
    val reviewRelTable = MorpheusElementTable.create(RelationshipMappingBuilder.on(sourceIdKey)
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

    // Create property graph
    morpheus.graphs.create(businessNodeTable, userNodeTable, reviewRelTable)
  }
}
