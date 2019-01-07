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

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates multiple graph capabilities by loading a social network from case class objects and a purchase network
  * from CSV data and schema files. The example connects both networks via matching user and customer names. A Cypher
  * query is then used to compute products that friends have bought.
  */
object MultipleGraphExample extends ConsoleApp {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
  session.catalog.store("socialNetwork", socialNetwork)

  // 3) Register a file system graph source to the catalog
  // Note: if files were stored in HDFS, the file path would indicate so by starting with hdfs://
  val csvFolder = getClass.getResource("/fs-graphsource/csv").getFile
  session.registerSource(Namespace("purchases"), GraphSources.fs(rootPath = csvFolder).csv)
  // access the graph from the catalog via its qualified graph name
  val purchaseNetwork = session.catalog.graph("purchases.products")

  // 5) Create new edges between users and customers with the same name
  val recommendationGraph = session.cypher(
    """|FROM GRAPH socialNetwork
       |MATCH (p:Person)
       |FROM GRAPH purchases.products
       |MATCH (c:Customer)
       |WHERE p.name = c.name
       |CONSTRUCT ON socialNetwork, purchases.products
       |  CREATE (p)-[:IS]->(c)
       |RETURN GRAPH
    """.stripMargin
  ).graph

  // 6) Query for product recommendations
  val recommendations = recommendationGraph.cypher(
    """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
       |      (friend)-[:IS]->(customer:Customer),
       |      (customer)-[:BOUGHT]->(product:Product)
       |RETURN DISTINCT product.title AS recommendation, person.name AS for
       |ORDER BY recommendation
    """.stripMargin)

  recommendations.show
}
// end::full-example[]
