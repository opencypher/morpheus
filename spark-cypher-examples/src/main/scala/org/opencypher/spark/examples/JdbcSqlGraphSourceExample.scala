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

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.{CensusDB, ConsoleApp}

object JdbcSqlGraphSourceExample extends ConsoleApp {

  implicit val resourceFolder: String = "/census"

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Create the data in H2 in-memory database
  implicit val sparkSession: SparkSession = session.sparkSession
  val schema = "CENSUS"
  val driver = "org.h2.Driver"
  val jdbcUrl = s"jdbc:h2:mem:$schema.db;INIT=CREATE SCHEMA IF NOT EXISTS $schema;DB_CLOSE_DELAY=30;"
  CensusDB.createJdbcData(driver, jdbcUrl, schema)

  // Register a SQL source (for JDBC) in the Cypher session
  val graphName = "Census_1901"
  val sqlGraphSource = GraphSources
    .sql(resource("ddl/census.ddl").getFile)
    .withSqlDataSourceConfigs(resource("ddl/jdbc-data-sources.json").getFile)
  session.registerSource(Namespace("sql"), sqlGraphSource)

  // Access the graph via its qualified graph name
  val census = session.catalog.graph("sql." + graphName)

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
}
// end::full-example[]
