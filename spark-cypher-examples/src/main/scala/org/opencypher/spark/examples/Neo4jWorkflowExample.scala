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

import org.opencypher.okapi.api.graph.{Namespace, QualifiedGraphName}
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.testing.Neo4jHarnessUtils._
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.testing.support.creation.CAPSNeo4jHarnessUtils._
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates connecting a graph from a CSV data source with a graph from a Neo4j data source.
  *
  * Writes updates back to the Neo4j database with Cypher queries.
  */
object Neo4jWorkflowExample extends ConsoleApp {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Start a Neo4j instance and populate it with social network data
  val neo4j = startNeo4j(personNetwork).withSchemaProcedure

  // Register Property Graph Data Sources (PGDS)
  session.registerSource(Namespace("socialNetwork"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))
  session.registerSource(Namespace("purchases"), GraphSources.fs(rootPath = getClass.getResource("/fs-graphsource/csv").getFile).csv)

  // Access the graphs via their qualified graph names
  val socialNetwork = session.catalog.graph("socialNetwork.graph")
  val purchaseNetwork = session.catalog.graph("purchases.products")

  // Build new recommendation graph that connects the social and product graphs and
  // create new edges between users and customers with the same name
  val recommendationGraph = session.cypher(
    """|FROM GRAPH socialNetwork.graph
       |MATCH (p:Person)
       |FROM GRAPH purchases.products
       |MATCH (c:Customer)
       |WHERE p.name = c.name
       |CONSTRUCT
       |  ON socialNetwork.graph, purchases.products
       |  CREATE (p)-[:IS]->(c)
       |RETURN GRAPH
    """.stripMargin
  ).getGraph.get

  // Query for product recommendations
  val recommendations = recommendationGraph.cypher(
    """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
       |(friend)-[:IS]->(customer:Customer),
       |(customer)-[:BOUGHT]->(product:Product)
       |RETURN person.name AS for, collect(DISTINCT product.title) AS recommendations""".stripMargin)

  // Use Cypher queries to write the product recommendations back to Neo4j
  recommendations.records.collect.foreach { recommendation =>
    neo4j.execute(
      s"""|MATCH (p:Person {name: ${recommendation.get("for").get.toCypherString}})
          |SET p.should_buy = ${recommendation.get("recommendations").get.toCypherString}""".stripMargin)
  }

  // Proof that the write-back to Neo4j worked, retrieve and print updated Neo4j results
  val updatedNeo4jSource = GraphSources.cypher.neo4j(neo4j.dataSourceConfig)
  session.registerSource(Namespace("updated-neo4j"), updatedNeo4jSource)
  val socialNetworkWithRanks = session.catalog.graph(QualifiedGraphName(Namespace("updated-neo4j"), entireGraphName))
  socialNetworkWithRanks.cypher("MATCH (p) WHERE p.should_buy IS NOT NULL RETURN p.name, p.should_buy").show

  // Shutdown Neo4j test instance
  neo4j.stop()

  def personNetwork =
    s"""|CREATE (a:Person { name: 'Alice', age: 10 })
        |CREATE (b:Person { name: 'Bob', age: 20})
        |CREATE (c:Person { name: 'Carol', age: 15})
        |CREATE (a)-[:FRIEND_OF { since: '23/01/1987' }]->(b)
        |CREATE (b)-[:FRIEND_OF { since: '12/12/2009' }]->(c)""".stripMargin
}
// end::full-example[]
