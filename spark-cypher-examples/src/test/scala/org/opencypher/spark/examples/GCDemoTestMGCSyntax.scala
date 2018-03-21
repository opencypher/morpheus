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

import java.net.URI

import org.apache.http.client.utils.URIBuilder
import org.neo4j.driver.v1.{AuthTokens, Session}
import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{MiniDFSClusterFixture, Neo4jServerFixture, SparkSessionFixture}

class GCDemoTestMGCSyntax extends CAPSTestSuite with SparkSessionFixture with Neo4jServerFixture with MiniDFSClusterFixture {

  val isChecking = false

  test("the demo") {
    val t0 = System.currentTimeMillis()

    // register pgds
    def nodeQuery(region: String) = s"MATCH (n {region: '$region'}) RETURN n"
    def relQuery(region: String) = s"MATCH ()-[r {region: '$region'}]->() RETURN r"
    caps.registerSource(Namespace("neo4j"), new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      GraphName("US") -> (nodeQuery("US") -> relQuery("US")),
      GraphName("EU") -> (nodeQuery("EU") -> relQuery("EU")))
    ))
    caps.registerSource(Namespace("hdfs"), HdfsCsvPropertyGraphDataSource(clusterConfig, rootPath = "/csv"))

    // query 1, find friends in the US social network
    caps.cypher(
      """CREATE GRAPH usFriends {
        |  USE GRAPH neo4j.US
        |  MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
        |  WHERE city.name = 'New York City' OR city.name = 'San Francisco'
        |  CONSTRUCT ON neo4j.US {
        |    MERGE (a)-[:CLOSE_TO]->(b)
        |  }
        |  RETURN GRAPH
        |}
      """.stripMargin)

    // query 2, find friends in the EU social network
    caps.cypher(
      """CREATE GRAPH euFriends {
        |  USE GRAPH neo4j.EU
        |  MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
        |  WHERE city.name = 'Malmö' OR city.name = 'Berlin'
        |  CONSTRUCT ON neo4j.EU {
        |    MERGE (a)-[:CLOSE_TO]->(b)
        |  }
        |  RETURN GRAPH
        |}
      """.stripMargin)

    // query 3, put these graphs together
    caps.cypher(
      """CREATE GRAPH allFriends {
        |  USE GRAPH euFriends
        |  RETURN GRAPH
        |  UNION ALL
        |  USE GRAPH usFriends
        |  RETURN GRAPH
        |}
      """.stripMargin
    )

    // query 4, connect the friends together with their purchases as customers
    caps.cypher(
      s"""CREATE GRAPH connectedCustomers {
         |  USE GRAPH allFriends
         |  MATCH (p:Person)
         |  WITH p.name AS personName, p
         |  USE GRAPH hdfs.prod
         |  MATCH (c:Customer)
         |  WITH c.name as customerName, personName, c, p
         |  WHERE customerName = personName
         |  CONSTRUCT ON hdfs.prod, allFriends {
         |    MERGE (c)-[:IS]->(p)
         |  }
         |  RETURN GRAPH
         |}
      """.stripMargin)

    // query 5, find people who are close to one another and compute recommendations for them
    val result = caps.cypher(
      """USE GRAPH connectedCustomers
        |MATCH (a:Person)-[:CLOSE_TO]-(b:Person)-[:HAS_INTEREST]->(i:Interest),
        |      (a)<-[:IS]-(x:Customer)-[r:BOUGHT]->(p:Product {category: i.name})
        |WHERE r.rating >= 4 AND (r.helpful * 1.0) / r.votes > 0.6
        |WITH * ORDER BY p.rank
        |RETURN DISTINCT p.title AS product, b.name AS name
        |LIMIT 100
      """.stripMargin)

    // print the results
    result.show

    // write back recommendations to social network
    withBoltSession { session =>
      result.getRecords.collect.foreach { cypherMap =>
        session.run(
          s"MATCH (p:Person {name: ${cypherMap.get("name").get}}) SET p.should_buy = ${cypherMap.get("product").get}")
      }
    }

    val tx = System.currentTimeMillis()
    System.out.println(s"${tx - t0} ms")
  }

  protected override def hdfsURI: URI = new URIBuilder(super.hdfsURI).build()

  protected override def dfsTestGraphPath = Some("/csv/prod")

  private def withBoltSession[T](f: Session => T): T = {
    val driver = org.neo4j.driver.v1.GraphDatabase
      .driver(neo4jHost, AuthTokens.basic(neo4jConfig.user, neo4jConfig.password.get), neo4jConfig.boltConfig())

    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }

  override def dataFixture =
    """
       CREATE (nyc:City {name: "New York City", region: "US"})
       CREATE (sfo:City {name: "San Francisco", region: "US"})
       CREATE (ams:City {name: "Amsterdam", region: "EU"})
       CREATE (mal:City {name: "Malmö", region: "EU"})
       CREATE (ber:City {name: "Berlin", region: "EU"})

       CREATE (loner:Person   {name: "Loner", region: "US"}  )-[:LIVES_IN {region: "US"}]->(chi)
       CREATE (alice:Person   {name: "Alice", region: "US"}  )-[:LIVES_IN {region: "US"}]->(nyc)
       CREATE (bob:Person     {name: "Bob", region: "US"}    )-[:LIVES_IN {region: "US"}]->(nyc)
       CREATE (eve:Person     {name: "Eve", region: "US"}    )-[:LIVES_IN {region: "US"}]->(nyc)
       CREATE (carol:Person   {name: "Carol", region: "US"}  )-[:LIVES_IN {region: "US"}]->(sfo)
       CREATE (carl:Person    {name: "Carl", region: "US"}   )-[:LIVES_IN {region: "US"}]->(sfo)
       CREATE (dave:Person    {name: "Dave", region: "US"}   )-[:LIVES_IN {region: "US"}]->(sfo)
       CREATE (mallory:Person {name: "Mallory", region: "EU"})-[:LIVES_IN {region: "EU"}]->(mal)
       CREATE (trudy:Person   {name: "Trudy", region: "EU"}  )-[:LIVES_IN {region: "EU"}]->(mal)
       CREATE (trent:Person   {name: "Trent", region: "EU"}  )-[:LIVES_IN {region: "EU"}]->(mal)
       CREATE (oscar:Person   {name: "Oscar", region: "EU"}  )-[:LIVES_IN {region: "EU"}]->(ber)
       CREATE (victor:Person  {name: "Victor", region: "EU"} )-[:LIVES_IN {region: "EU"}]->(ber)
       CREATE (peggy:Person   {name: "Peggy", region: "EU"}  )-[:LIVES_IN {region: "EU"}]->(ber)
       CREATE (euLoner:Person {name: "EuLoner", region: "EU"})-[:LIVES_IN {region: "EU"}]->(ams)

       CREATE (eve)<-[:KNOWS {region: "US"}]-(alice)-[:KNOWS {region: "US"}]->(bob)-[:KNOWS {region: "US"}]->(eve)
       CREATE (carol)-[:KNOWS {region: "US"}]->(carl)-[:KNOWS {region: "US"}]->(dave)
       CREATE (mallory)-[:KNOWS {region: "EU"}]->(trudy)-[:KNOWS {region: "EU"}]->(trent)
       CREATE (peggy)-[:KNOWS {region: "EU"}]->(oscar)-[:KNOWS {region: "EU"}]->(victor)

       CREATE (book_US:Interest  {name: "Book", region: "US"})
       CREATE (dvd_US:Interest   {name: "DVD", region: "US"})
       CREATE (video_US:Interest {name: "Video", region: "US"})
       CREATE (music_US:Interest  {name: "Music", region: "US"})

       CREATE (book_EU:Interest  {name: "Book", region: "EU"})
       CREATE (dvd_EU:Interest   {name: "DVD", region: "EU"})
       CREATE (video_EU:Interest {name: "Video", region: "EU"})
       CREATE (music_EU:Interest  {name: "Music", region: "EU"})

       CREATE (bob)-[:HAS_INTEREST {region: "US"}]->(book_US)
       CREATE (eve)-[:HAS_INTEREST {region: "US"}]->(dvd_US)
       CREATE (carl)-[:HAS_INTEREST {region: "US"}]->(video_US)
       CREATE (dave)-[:HAS_INTEREST {region: "US"}]->(music_US)
       CREATE (trudy)-[:HAS_INTEREST {region: "EU"}]->(book_EU)
       CREATE (eve)-[:HAS_INTEREST {region: "EU"}]->(dvd_EU)
       CREATE (victor)-[:HAS_INTEREST {region: "EU"}]->(video_EU)
       CREATE (peggy)-[:HAS_INTEREST {region: "EU"}]->(music_EU)
    """
}
