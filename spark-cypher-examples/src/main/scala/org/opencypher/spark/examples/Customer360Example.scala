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
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport.entireGraphName
import org.opencypher.okapi.neo4j.io.testing.Neo4jUtils._
import org.opencypher.spark.api.io.neo4j.sync.Neo4jGraphMerge
import org.opencypher.spark.api.io.neo4j.sync.Neo4jGraphMerge.Batches
import org.opencypher.spark.api.io.sql.IdGenerationStrategy
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.Hive
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.{ConsoleApp, LoadInteractionsInHive}

/**
  * Example: Customer360
  *
  * A business has a record of interactions between its customers and its employed customer reps.
  * This record is modelled as a stream of events that get logged in a simple CSV file.
  * As time progresses, more data is added to the file.
  *
  * A graph perspective is applied to this dataset, where we can identify customers who interact with
  * customer reps with policies and accounts.
  * From a business analysis perspective, we are interested in finding out which customers and which
  * customer reps are involved in the most problematic interactions, such as cancellations.
  *
  * Using Graph DDL we load a set of Hive views into a graph, which we seed into a Neo4j transactional database.
  * As time progresses, we need to update our transactional database with the incoming deltas, which are taken
  * at regular time intervals.
  * In this demo we showcase one such time interval, and merge a delta graph into the Neo4j database.
  *
  * One may observe that the Cypher queries shown here may also be executed directly in Neo4j, with the same results.
  */
object Customer360Example extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Load a CSV file of interactions into Hive tables (views)
  LoadInteractionsInHive.load()

  // Create a SQL Property Graph Data Source (PGDS)
  // This is configured to read from Hive via its SQL layer
  // It uses a Graph DDL file for understanding schema and data of its managed graphs
  val sqlPgds = GraphSources
    .sql(file("/customer-interactions/ddl/customer-interactions.ddl"))
    .withIdGenerationStrategy(IdGenerationStrategy.HashedId)
    .withSqlDataSourceConfigs(Map("hive_interactions" -> Hive))

  // Register the SQL PGDS in the session's catalog
  // The namespace is used for referencing graphs from this PGDS
  session.registerSource(Namespace("c360"), sqlPgds)

  // Start an empty Neo4j database
  val neo4j = startNeo4j("")

  // Register a Neo4j PGDS in the session's catalog
  // This enables reading graphs from a Neo4j database into Morpheus
  session.registerSource(Namespace("transactional"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))

  // Register a file system PGDS in the session's catalog
  // This allows storing snapshots of graphs persistently for processing in later Morpheus sessions
  // There are multiple file formats to use; here we use the binary Parquet format
  // We also integrate this with Hive so that the dataframes stored on disk are also visible in Hive views
  // To make this work we ensure that there the Hive database exists
  session.sql("CREATE DATABASE IF NOT EXISTS snapshots")
  session.registerSource(Namespace("snapshots"),
    GraphSources.fs("snapshots-root", Some("snapshots")).parquet
  )

  println("PGDSs registered")

  val c360Seed = session.cypher(
    """
      |FROM c360.interactions_seed
      |RETURN GRAPH
    """.stripMargin).graph

  /*
   * Find customers who have reported the most problematic interactions (complaints or cancellations)
   * List the top 6 customers and their interaction statistics
   */
  c360Seed.cypher(
    """
      |MATCH (c:Customer)--(i:Interaction)--(:CustomerRep)
      |WITH c, i.type AS type, count(*) AS cnt
      |WHERE type IN ['cancel', 'complaint']
      |RETURN c, type, cnt
      |ORDER BY cnt DESC
      |LIMIT 42
    """.stripMargin).show

  // Speed up merge operation. Requires Neo4j Enterprise Edition
//  Neo4jGraphMerge.createIndexes(entireGraphName, neo4j.dataSourceConfig, c360Seed.schema.nodeKeys)

  // Seed Neo4j with the customer 360 graph
  Neo4jGraphMerge.merge(entireGraphName, c360Seed, neo4j.dataSourceConfig, batches = Batches(relBatchSize = 1))

  println("Graph merged to Neo4j")

  /*
   * We can also execute the same query based on the graph we merged into the Neo4j instance, seeing the same results.
   */
  session.cypher(
    s"""
       |FROM transactional.$entireGraphName
       |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
       |WITH c, i.type AS type, count(*) AS cnt
       |WHERE type IN ['cancel', 'complaint']
       |RETURN c, type, cnt
       |ORDER BY cnt DESC
       |LIMIT 42
    """.stripMargin).show

  /*
   * Find customer reps who have received the most problematic reports (complaints or cancellations)
   * List the top 9 customer reps and their interaction statistics
   */
  session.cypher(
    """
      |FROM c360.interactions_seed
      |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
      |WITH rep, i.type AS type, count(*) AS cnt
      |WHERE type IN ['cancel', 'complaint']
      |RETURN rep, type, cnt
      |ORDER BY cnt DESC
      |LIMIT 12
    """.stripMargin).show

  /*
   * We can also execute the same query based on the graph we merged into the Neo4j instance, seeing the same results.
   */
  session.cypher(
    s"""
       |FROM transactional.$entireGraphName
       |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
       |WITH rep, i.type AS type, count(*) AS cnt
       |WHERE type IN ['cancel', 'complaint']
       |RETURN rep, type, cnt
       |ORDER BY cnt DESC
       |LIMIT 12
    """.stripMargin).show

  // Time moves forward and more interactions happen
  // We want to synchronize our transactional database with the new data
  // Here we read the graph directly from the catalog
  Neo4jGraphMerge.merge(
    entireGraphName,
    session.catalog.graph("c360.interactions_delta"),
    neo4j.dataSourceConfig,
    batches = Batches(relBatchSize = 1)
  )

  println("Delta merged")

  // Find the updated statistics on customer rep interactions
  // Here we execute the query from Spark by importing the necessary data from Neo4j on the fly
  session.cypher(
    s"""
       |FROM transactional.$entireGraphName
       |MATCH (c:Customer)--(i:Interaction)--(rep:CustomerRep)
       |WITH rep, i.type AS type, count(*) AS cnt
       |WHERE type IN ['cancel', 'complaint']
       |RETURN rep, type, cnt
       |ORDER BY cnt DESC
       |LIMIT 19
    """.stripMargin).show

  neo4j.close()
  session.sparkSession.close()

  private def file(path: String): String = {
    getClass.getResource(path).toURI.getPath
  }

}
