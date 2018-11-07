/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
// tag::full-example[]
package org.opencypher.spark.examples

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.{CensusDB, ConsoleApp}

object CensusHiveExample extends ConsoleApp {

  implicit val resourceFolder: String = "/census"

  implicit val session: CAPSSession = CAPSSession.local(hiveExampleSettings: _*)
  implicit val sparkSession: SparkSession = session.sparkSession
  // end::create-session[]


  // tag::register-sql-source-in-session[]
  // Register a SQL source (for Hive) in the Cypher session
  val graphName = "Census_1901"
  val sqlGraphSource = GraphSources
      .sql(resource("ddl/census.ddl").getFile)
      .withSqlDataSourceConfigs(resource("ddl/hive-data-sources.json").getFile)

  // tag::prepare-sql-database[]
  // Create the data in H2 in-memory database
  CensusDB.createHiveData(sqlGraphSource.sqlDataSourceConfigs.find(_.dataSourceName == "CENSUS").get)
  // end::prepare-sql-database[]

  session.registerSource(Namespace("sql"), sqlGraphSource)
  // end::register-sql-source-in-session[]

  // tag::access-registered-graph[]
  // Access the graph via its qualified graph name
  val census = session.catalog.graph("sql." + graphName)
  // end::access-registered-graph[]

  // tag::query-graph[]
  // Run a simple Cypher query
  census.cypher(
    s"""
       |FROM GRAPH sql.$graphName
       |MATCH (n:Person)-[r]->(m)
       |WHERE n.age >= 30
       |RETURN n,r,m
       |ORDER BY n.age
    """.stripMargin)
    .records
    .show
  // end::query-graph[]

  // Set up temporary spark and hive directories for this example
  private def hiveExampleSettings: Seq[(String, String)] = {
    val sparkWarehouseDir = new File(s"spark-warehouse_${System.currentTimeMillis}").getAbsolutePath
    Seq(
      // ------------------------------------------------------------------------
      // Create a new unique local spark warehouse dir for every run - idempotent
      // ------------------------------------------------------------------------
      ("spark.sql.warehouse.dir", sparkWarehouseDir),
      // -----------------------------------------------------------------------------------------------------------
      // Create an in-memory Hive Metastore (only Derby supported for this mode)
      // This is to avoid creating a local HIVE "metastore_db" on disk which needs to be cleaned up before each run,
      // e.g. avoids database and table already exists exceptions on re-runs - not to be used for production.
      // -----------------------------------------------------------------------------------------------------------
      ("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:hms;create=true"),
      ("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver"),
      // ------------------------------------------------------------------------------------------------------------
      // An alternative way of enabling Spark Hive Support (e.g. you could use enableHiveSupport on the SparkSession)
      // ------------------------------------------------------------------------------------------------------------
      ("hive.metastore.warehouse.dir", s"warehouse_${System.currentTimeMillis}"),
      (CATALOG_IMPLEMENTATION.key, "hive") // Enable hive
    )
  }

}
// end::full-example[]
