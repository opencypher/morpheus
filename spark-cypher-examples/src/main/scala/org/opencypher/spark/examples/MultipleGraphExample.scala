/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.file.FileCsvPropertyGraphDataSource

/**
  * Demonstrates multiple graph capabilities by loading a social network from case class objects and a purchase network
  * from CSV data and schema files. The example connects both networks via matching user and customer names. A Cypher
  * query is then used to compute products that friends have bought.
  */
object MultipleGraphExample extends App {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Register a File-based data source in the Cypher session
  val csvFolder = getClass.getResource("/csv").getFile
  // Note: if files were stored in HDFS, change the data source to HdfsCsvPropertyGraphDataSource
  session.registerSource(Namespace("myDataSource"), new FileCsvPropertyGraphDataSource(rootPath = csvFolder))
  // access the graph via its qualified graph name
  val purchaseNetwork = session.graph(QualifiedGraphName(Namespace("myDataSource"), GraphName("prod")))

  // 4) Build union of social and purchase network (note, that there are no relationships connecting nodes from both graphs)
  val disconnectedGraph = socialNetwork union purchaseNetwork

  // 5) Create new edges between users and customers with the same name
  val integrationGraph = disconnectedGraph.cypher(
    """|MATCH (p:Person),(c:Customer)
       |WHERE p.name = c.name
       |RETURN GRAPH OF (p)-[x:IS]->(c)
    """.stripMargin
  ).graph.get

  // 6) Build recommendation graph from disconnected and integration graphs
  val recommendationGraph = disconnectedGraph union integrationGraph

  // 7) Query for product recommendations
  val recommendations = recommendationGraph.cypher(
    """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
       |(friend)-[:IS]->(customer:Customer),
       |(customer)-[:BOUGHT]->(product:Product)
       |RETURN DISTINCT product.title AS recommendation, person.name AS for
    """.stripMargin)

  recommendations.show
}
