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
// tag::full-example[]
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.{Namespace, QualifiedGraphName}
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.testing.Neo4jUtils._
import org.opencypher.spark.api.io.neo4j.sync.Neo4jGraphMerge
import org.opencypher.spark.api.{CAPSSession, GraphSources}
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
  val neo4j = startNeo4j(personNetwork)

  // Register Property Graph Data Sources (PGDS)
  session.registerSource(Namespace("socialNetwork"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))
  session.registerSource(Namespace("purchases"), GraphSources.fs(rootPath = getClass.getResource("/fs-graphsource/csv").getFile).csv)

  // Access the graphs via their qualified graph names
  val socialNetwork = session.catalog.graph("socialNetwork.graph")
  val purchaseNetwork = session.catalog.graph("purchases.products")

  // Build new integrated graph that connects the social and product graphs and
  // create new edges between users and customers with the same name
  val integratedGraph = session.cypher(
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

  // Query for product recommendations and create new edges between users and the product they should buy
  val recommendationGraph = integratedGraph.cypher(
    """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
       |      (friend)-[:IS]->(customer:Customer),
       |      (customer)-[:BOUGHT]->(product:Product)
       |CONSTRUCT
       |      CREATE (person)-[:SHOULD_BUY]->(product)
       |RETURN GRAPH""".stripMargin).graph

  // Use the Neo4jGraphMerge utility to write the products and the recommendations back to Neo4j

  // Define the node and relationship keys
  // Note that in this example we assume that names and titles uniquely identify people and products
  val nodeKeys = Map("Person" -> Set("name"), "Product" -> Set("title"))

  // Write the recommendations back to Neo4j
  Neo4jGraphMerge.merge(entireGraphName, recommendationGraph, neo4j.dataSourceConfig, Some(nodeKeys))

  // Proof that the write-back to Neo4j worked, retrieve and print updated Neo4j results
  val updatedNeo4jSource = GraphSources.cypher.neo4j(neo4j.dataSourceConfig)
  session.registerSource(Namespace("updated-neo4j"), updatedNeo4jSource)
  val socialNetworkWithRanks = session.catalog.graph(QualifiedGraphName(Namespace("updated-neo4j"), entireGraphName))
  socialNetworkWithRanks.cypher(
    """MATCH (person:Person)-[:SHOULD_BUY]->(product:Product)
      |RETURN person.name AS person, product.title AS should_buy
      |ORDER BY person, should_buy""".stripMargin).show

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
