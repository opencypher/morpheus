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
package org.opencypher.spark.integration

import java.net.URI

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.spark.api.io.neo4j.sync.Neo4jGraphMerge
import org.opencypher.spark.api.{MorpheusSession, GraphSources}
import org.opencypher.spark.util.App

/**
  * This demo shows the transformation from a single CSV file into a PropertyGraph that is eventually stored in Neo4j.
  *
  * The input is a de-normalized wide table containing interactions between customers and employees. Those interactions
  * are normalized by splitting them into several views and registering them as Hive tables.
  *
  * A Graph DDL file describes the PropertyGraph schema and the mapping between Hive views and PropertyGraph elements
  * (i.e. nodes and relationships). After loading the graph via Spark, Cypher 10 Multiple Graph support is used to
  * extend the graph with new relationships. The resulting graph is merged to Neo4j where it can be further analyzed.
  *
  * This integration demo requires a running Neo4j Enterprise installation listening on bolt://localhost:7687.
  */
object Customer360IntegrationDemo extends App {

  implicit val resourceFolder: String = "/customer-interactions"

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  implicit val session: MorpheusSession = MorpheusSession.local(CATALOG_IMPLEMENTATION.key -> "hive")

  val inputSchema: StructType = StructType(Seq(
    StructField("interactionId", LongType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("customerIdx", LongType, nullable = false),
    StructField("empNo", LongType, nullable = false),
    StructField("empName", StringType, nullable = false),
    StructField("type", StringType, nullable = false),
    StructField("outcomeScore", StringType, nullable = false),
    StructField("accountHolderId", StringType, nullable = false),
    StructField("policyAccountNumber", StringType, nullable = false),
    StructField("customerId", StringType, nullable = false),
    StructField("customerName", StringType, nullable = false)
  ))

  // Import wide table from CSV file
  val importCsv = spark.read
    .format("csv")
    .option("header", "true")
    .schema(inputSchema)
    .load(resource("csv/customer-interactions.csv").getFile)

  importCsv.show()

  val databaseName = "customers"
  val inputTableName = s"$databaseName.csv_input"

  spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")
  spark.sql(s"CREATE DATABASE $databaseName")

  importCsv.write.saveAsTable(s"$inputTableName")

  // Create views for nodes
  createView(inputTableName, "interactions", true, "interactionId", "date", "type", "outcomeScore")
  createView(inputTableName, "customers", true, "customerIdx", "customerId", "customerName")
  createView(inputTableName, "account_holders", true, "accountHolderId")
  createView(inputTableName, "policies", true, "policyAccountNumber")
  createView(inputTableName, "customer_reps", true, "empNo", "empName")

  // Create views for relationships
  createView(inputTableName, "has_customer_reps", false, "interactionId", "empNo")
  createView(inputTableName, "has_customers", false, "interactionId", "customerId")
  createView(inputTableName, "has_policies", false, "interactionId", "policyAccountNumber")
  createView(inputTableName, "has_account_holders", false, "interactionId", "accountHolderId")

  // Create SQL PropertyGraph DataSource
  val sqlGraphSource = GraphSources
    .sql(resource("ddl/customer-interactions.ddl").getFile)
    .withSqlDataSourceConfigs(resource("ddl/data-sources.json").getFile)

  session.registerSource(Namespace("c360"), sqlGraphSource)

  val g = session.catalog.graph("c360.interactions")
  session.catalog.store("foo", g)

  val c360Interactions = session.cypher(
    """
      |FROM GRAPH foo
      |MATCH (i:Interaction)-[rel]->(other)
      |CONSTRUCT ON foo
      | CREATE (other)-[:HAS_INTERACTION]->(i)
      |RETURN GRAPH
    """.stripMargin).graph

  val neo4jConfig = Neo4jConfig(new URI("bolt://localhost:7687"), "neo4j", Some("admin"))

  val nodeKeys = Map(
    "Interaction" -> Set("interactionId"),
    "Customer" -> Set("customerId"),
    "CustomerRep" -> Set("empNo"),
    "AccountHolder" -> Set("accountHolderId"),
    "Policy" -> Set("policyAccountNumber")
  )

  // Speed up merge operation (requires Neo4j Enterprise Edition)
  Neo4jGraphMerge.createIndexes(entireGraphName, neo4jConfig, nodeKeys)

  // Merge interaction graph into Neo4j database with "Customer 360" data
  Neo4jGraphMerge.merge(entireGraphName, c360Interactions, neo4jConfig, Some(nodeKeys))

  def createView(fromTable: String, viewName: String, distinct: Boolean, columns: String*): Unit = {
    val distinctString = if (distinct) "DISTINCT" else ""

    spark.sql(
      s"""
         |CREATE VIEW $databaseName.$viewName AS
         | SELECT $distinctString ${columns.mkString(", ")} FROM $fromTable
      """.stripMargin)
  }
}
