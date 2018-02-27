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

import org.neo4j.driver.v1.{AuthTokens, Session}
import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.file.FileCsvPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource._
import org.opencypher.spark.api.io.neo4j.{Neo4jConfig, Neo4jPropertyGraphDataSource}

/**
  * Demonstrates connecting a graph from a CSV data source with a graph from a Neo4j data source.
  *
  * Write updates back to the Neo4j database with Cypher queries.
  */
object Neo4jMultipleGraphWorkflow extends App {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load a graph from a running Neo4j instance. Setup with Neo4j Desktop from https://neo4j.com/download/
  // Load test data into Neo4j
  import Neo4jHelpers._
  //  withBoltSession(loadPersonNetwork)

  // Remove the next line and set/store the PW property separately from the application source code.
  System.setProperty("neo4j-pw", "example-pw") // Remove: Do not store passwords in code, set them externally instead.
  val neo4jPw = System.getProperty("neo4j-pw")
  implicit val neo4jConfig = Neo4jConfig(password = Some(neo4jPw))
  val neo4jSource = new Neo4jPropertyGraphDataSource(neo4jConfig)
  val neo4jNamespace = Namespace("neo4j")
  session.registerSource(neo4jNamespace, neo4jSource)
  // Access then graph via its qualified graph name
  val socialNetwork = session.graph(QualifiedGraphName(neo4jNamespace, neo4jDefaultGraphName))

  // 3) Register a File-based data source in the Cypher session
  val csvFolder = getClass.getResource("/csv").getFile
  val csvNamespace = Namespace("csv")
  session.registerSource(csvNamespace, new FileCsvPropertyGraphDataSource(rootPath = csvFolder))
  // Access the graph via its qualified graph name
  val purchaseNetwork = session.graph(QualifiedGraphName(csvNamespace, GraphName("prod")))

  // 4) Build union of Neo4j social and CSV purchase network
  //    (note that there are no relationships connecting nodes from both graphs)
  val disconnectedGraph = socialNetwork union purchaseNetwork

  // 5) Create new edges between users and customers with the same name
  val integrationGraph = disconnectedGraph.cypher(
    """|FROM GRAPH AT neo4j.graph
       |MATCH (p:Person)
       |FROM GRAPH AT csv.prod
       |MATCH (c:Customer)
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

  // 8) Use Cypher queries to write the product recommendations back to Neo4j
  withBoltSession { session =>
    recommendations.records.collect.foreach { recommendation =>
      session.run(
        s"""|MATCH (p:Person {name: ${recommendation.get("for").get.toCypherString}})
            |SET p.should_buy = ${recommendation.get("recommendation").get.toCypherString}""".stripMargin)
    }
  }

}

object Neo4jHelpers {

  def loadPersonNetwork(session: Session)(implicit neo4jConfig: Neo4jConfig): Unit = {
    session.run(
      s"""|CREATE (a:Person { name: 'Alice', age: 10 })
          |CREATE (b:Person { name: 'Bob', age: 20})
          |CREATE (c:Person { name: 'Carol', age: 20})
          |CREATE (a)-[:FRIEND_OF { since: '23/01/1987' }]->(b)
          |CREATE (b)-[:FRIEND_OF { since: '12/12/2009' }]->(c)""".stripMargin)
  }

  def withBoltSession[T](f: Session => T)(implicit neo4jConfig: Neo4jConfig): T = {
    val driver = org.neo4j.driver.v1.GraphDatabase.driver(
      neo4jConfig.uri, AuthTokens.basic(neo4jConfig.user, neo4jConfig.password.get), neo4jConfig.boltConfig())
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }

}
