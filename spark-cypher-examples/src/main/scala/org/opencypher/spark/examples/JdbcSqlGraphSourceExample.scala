/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
