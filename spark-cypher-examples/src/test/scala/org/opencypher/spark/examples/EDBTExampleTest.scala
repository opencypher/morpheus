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
import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{MiniDFSClusterFixture, Neo4jServerFixture, SparkSessionFixture}

class EDBTExampleTest extends CAPSTestSuite with SparkSessionFixture with Neo4jServerFixture with MiniDFSClusterFixture {

  test("CAPS Multiple Graphs EDBT Demo") {
    // Register Property Graph Data Sources (PGDS)

    // Neo4j PGDS
    def nodeQuery(region: String) = s"MATCH (n {region: '$region'}) RETURN n"
    def relQuery(region: String) = s"MATCH ()-[r {region: '$region'}]->() RETURN r"
    caps.registerSource(Namespace("neo4j"), new Neo4jPropertyGraphDataSource(neo4jConfig, Map(
      GraphName("US") -> (nodeQuery("US") -> relQuery("US")),
      GraphName("EU") -> (nodeQuery("EU") -> relQuery("EU")))
    ))
    // HDFS CSV PDGS
    caps.registerSource(Namespace("hdfs"), HdfsCsvPropertyGraphDataSource(clusterConfig, rootPath = "/csv"))

    /**
      * Returns a query that creates a graph containing persons that live in the same city and
      * know each other via 1 to 2 hops. The created graph contains a CLOSE_TO relationship between
      * each such pair of persons and is stored in the session catalog using the given graph name.
      */
    def cityFriendsQuery(fromGraph: String, cities: List[String]): String =
      s"""FROM GRAPH $fromGraph
        |MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
        |WHERE ${cities.map(c => s"city.name = '$c'").mkString(" OR ")}
        |CONSTRUCT
        |  ON $fromGraph
        |  CLONE a, b
        |  NEW (a)-[:CLOSE_TO]->(b)
        |RETURN GRAPH
      """.stripMargin

    // Find persons that are close to each other in the US social network
    val usFriends = caps.cypher(cityFriendsQuery("neo4j.US", List("New York City", "San Francisco"))).getGraph
    // Find persons that are close to each other in the EU social network
    val euFriends = caps.cypher(cityFriendsQuery("neo4j.EU", List("Malmö", "Berlin"))).getGraph

    // Union the US and EU graphs into a single graph 'allFriends' and store it in the session.
    val allFriendsName = caps.store(GraphName("allFriends"), usFriends.unionAll(euFriends))

    // Connect the social network with the products network using equal person and customer emails.
    val connectedCustomers = caps.cypher(
      s"""FROM GRAPH $allFriendsName
         |MATCH (p:Person)
         |FROM GRAPH hdfs.prod
         |MATCH (c:Customer)
         |WHERE c.name = p.name
         |CONSTRUCT ON hdfs.prod, $allFriendsName
         |  CLONE c, p
         |  NEW (c)-[:IS]->(p)
         |RETURN GRAPH
      """.stripMargin).getGraph

    // Compute recommendations for 'target' based on their interests and what persons close to the
    // 'target' have already bought and given a helpful and positive rating.
    val recommendationTable = connectedCustomers.cypher(
      s"""MATCH (target:Person)<-[:CLOSE_TO]-(person:Person),
        |       (target)-[:HAS_INTEREST]->(i:Interest),
        |       (person)<-[:IS]-(x:Customer)-[b:BOUGHT]->(product:Product {category: i.name})
        |WHERE b.rating >= 4 AND (b.helpful * 1.0) / b.votes > 0.6
        |WITH * ORDER BY product.rank
        |RETURN DISTINCT product.title AS product, target.name AS name
        |LIMIT 100
      """.stripMargin).getRecords

    // Print the results
    recommendationTable.show
  }

  protected override def hdfsURI: URI = new URIBuilder(super.hdfsURI).build()

  protected override def dfsTestGraphPath = Some("/csv/prod")

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
