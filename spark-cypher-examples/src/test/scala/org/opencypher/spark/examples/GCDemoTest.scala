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

import java.net.{URI, URLEncoder}

import org.apache.http.client.utils.URIBuilder
import org.apache.spark.sql.Row
import org.neo4j.driver.v1.{AuthTokens, Session}
import org.opencypher.okapi.api.graph.{CypherResult, GraphName, Namespace, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._
import org.opencypher.spark.api.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.impl.{CAPSPatternGraph, CAPSRecords}
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{MiniDFSClusterFixture, Neo4jServerFixture, SparkSessionFixture}
import org.scalatest.Assertion

class GCDemoTest extends CAPSTestSuite with SparkSessionFixture with Neo4jServerFixture with MiniDFSClusterFixture {

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

    // query 1
    val CITYFRIENDS_US =
      caps.cypher(
        """USE GRAPH neo4j.US
          |MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
          |WHERE city.name = 'New York City' OR city.name = 'San Francisco'
          |CONSTRUCT {
          |  CREATE (a)-[:CLOSE_TO]->(b)
          |}
          |RETURN GRAPH
        """.stripMargin)

    // query 2
    val CITYFRIENDS_EU =
      caps.cypher(
        """USE GRAPH neo4j.EU
          |MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
          |WHERE city.name = 'Malmö' OR city.name = 'Berlin'
          |CONSTRUCT {
          |  CREATE (a)-[:CLOSE_TO]->(b)
          |}
          |RETURN GRAPH
        """.stripMargin)

    val ALL_CITYFRIENDS = CITYFRIENDS_EU.getGraph unionAll CITYFRIENDS_US.getGraph

    caps.store(GraphName("friends"), ALL_CITYFRIENDS)

    // query 3
    val LINKS = caps.cypher(
      s"""USE GRAPH friends
         |MATCH (p:Person)
         |WITH p.name AS personName, p
         |USE GRAPH hdfs.prod
         |MATCH (c:Customer)
         |WITH c.name as customerName, personName, c, p
         |WHERE customerName = personName
         |CONSTRUCT {
         |  CREATE (c)-[:IS]->(p)
         |}
         |RETURN GRAPH
      """.stripMargin).getGraph

    val PRODUCTS = caps.cypher("USE GRAPH hdfs.prod RETURN GRAPH").getGraph
    val SN_US = caps.cypher("USE GRAPH neo4j.US RETURN GRAPH").getGraph
    val SN_EU = caps.cypher("USE GRAPH neo4j.EU RETURN GRAPH").getGraph

    /*
     * This step is where it all comes together.
     *
     * An equivalence UNION collapses the CREATEd persons and customers above
     * into the same nodes, which connects the CLOSE_TO and IS relationships
     * as well as the KNOWS, LIVES_IN and HAS_INTEREST from the source social networks
     * and the BOUGHT from the product graph
     */
    val RECO = ALL_CITYFRIENDS unionAll PRODUCTS unionAll LINKS unionAll SN_EU unionAll SN_US

    // query 4
    val result = RECO.cypher(
      """MATCH (a:Person)-[:CLOSE_TO]-(b:Person)-[:HAS_INTEREST]->(i:Interest),
        |      (a)<-[:IS]-(x:Customer)-[r:BOUGHT]->(p:Product {category: i.name})
        |WHERE r.rating >= 4 AND (r.helpful * 1.0) / r.votes > 0.6
        |WITH * ORDER BY p.rank
        |RETURN DISTINCT p.title AS product, b.name AS name
        |LIMIT 100
      """.stripMargin)

    // print the results
    result.show

    //Write back to Neo
    withBoltSession { session =>
      // maybe iterate over rows instead of CypherMaps is faster
      result.getRecords.collect.foreach { cypherMap =>
        session.run(
          s"MATCH (p:Person {name: ${cypherMap.get("name").get}}) SET p.should_buy = ${cypherMap.get("product").get}")
      }
    }

    val tx = System.currentTimeMillis()
    System.out.println(s"${tx - t0} ms")
  }

  ignore("write back to Neo") {
    val SN_US = neoRegionGraph("US")
    val result = SN_US.cypher("""MATCH (n:Person {name: "Alice"}) RETURN n.name AS name""")
    withBoltSession { session =>
      result.getRecords.collect.foreach { cypherMap =>
        session.run(s"MATCH (p:Person {name: ${cypherMap.get("name").get.toCypherString}}) SET p.should_buy = 'a book'")
      }
    }

    val resultGraph = neoRegionGraph("US")
    val res = resultGraph.cypher("MATCH (n:Person {name: 'Alice'}) RETURN n.should_buy as rec")

    res.getRecords.collect.toSet should equal(
      Set(
        CypherMap("rec" -> "a book")
      ))
  }

  protected override def hdfsURI: URI = new URIBuilder(super.hdfsURI).build()

  protected override def dfsTestGraphPath = Some("/csv/prod")

  private def check(f: => Unit): Unit = {
    if (isChecking) f
  }

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

  private def neoRegionGraph(region: String): PropertyGraph = {
    val regionGraphName = GraphName(region)
    val nodeQuery = URLEncoder.encode(s"MATCH (n {region: '$region'}) RETURN n", "UTF-8")
    val relQuery = URLEncoder.encode(s"MATCH ()-[r {region: '$region'}]->() RETURN r", "UTF-8")
    new Neo4jPropertyGraphDataSource(neo4jConfig, Map(regionGraphName -> (nodeQuery -> relQuery)))
      .graph(regionGraphName)
  }

  def verifyRecoResult(r: CypherResult) = {
    println("===>>> verifying RECO result")

    r.getRecords.asInstanceOf[CAPSRecords].toCypherMaps.collect().toSet should equal(
      Set(
        CypherMap("name" -> "Eve", "product" -> "Terminator 2"),
        CypherMap("name" -> "Carl", "product" -> "Jurassic Park"),
        CypherMap("name" -> "Bob", "product" -> "1984"),
        CypherMap("name" -> "Trudy", "product" -> "Cryptonomicon"),
        CypherMap("name" -> "Dave", "product" -> "Shakira")
      ))
  }

  def verifyReco(graph: PropertyGraph) = {
    println("===>>> verifying RECO")

    graph.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
        .withNodePropertyKeys("Interest")("name" -> CTString, "region" -> CTString)
        .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTString)
        .withNodePropertyKeys("Customer")("name" -> CTString.nullable)
        .withNodePropertyKeys("Product")(
          "title" -> CTString.nullable,
          "rank" -> CTInteger.nullable,
          "category" -> CTString.nullable)
        .withRelationshipType("IS")
        .withRelationshipType("CLOSE_TO")
        .withRelationshipPropertyKeys("LIVES_IN")("region" -> CTString)
        .withRelationshipPropertyKeys("BOUGHT")(
          "rating" -> CTInteger.nullable,
          "helpful" -> CTInteger.nullable,
          "votes" -> CTInteger.nullable)
        .withRelationshipPropertyKeys("HAS_INTEREST")("region" -> CTString)
        .withRelationshipPropertyKeys("KNOWS")("region" -> CTString))

    graph.nodes("n").collect.toBag should equal(
      Bag(
        Row(2009L, false, false, true, false, false, "Trent", null, null, null, null),
        Row(1L, true, false, false, false, false, "San Francisco", null, "US", null, null),
        Row(10L, false, false, false, false, true, "Carol", null, "US", null, null),
        Row(2002L, false, false, true, false, false, "Bob", null, null, null, null),
        Row(1010L, false, false, false, true, false, null, 112L, null, "Video", "Das Boot"),
        Row(23L, false, true, false, false, false, "Music", null, "US", null, null),
        Row(1009L, false, false, false, true, false, null, 832L, null, "Video", "Jurassic Park"),
        Row(1006L, false, false, false, true, false, null, 820L, null, "DVD", "Die Hard 3"),
        Row(25L, false, true, false, false, false, "DVD", null, "EU", null, null),
        Row(1005L, false, false, false, true, false, null, 102L, null, "DVD", "Terminator 2"),
        Row(5L, false, false, false, false, true, "Loner", null, "US", null, null),
        Row(17L, false, false, false, false, true, "Victor", null, "EU", null, null),
        Row(2007L, false, false, true, false, false, "Mallory", null, null, null, null),
        Row(15L, false, false, false, false, true, "Trent", null, "EU", null, null),
        Row(13L, false, false, false, false, true, "Mallory", null, "EU", null, null),
        Row(1002L, false, false, false, true, false, null, 842L, null, "Book", "Cryptonomicon"),
        Row(2006L, false, false, true, false, false, "Dave", null, null, null, null),
        Row(1007L, false, false, false, true, false, null, 152L, null, "DVD", "Matrix"),
        Row(1013L, false, false, false, true, false, null, 886L, null, "Music", "Shakira"),
        Row(18L, false, false, false, false, true, "Peggy", null, "EU", null, null),
        Row(2010L, false, false, true, false, false, "Oscar", null, null, null, null),
        Row(1003L, false, false, false, true, false, null, 950L, null, "Book", "The Eye of the World"),
        Row(1008L, false, false, false, true, false, null, 927L, null, "DVD", "Iron Man"),
        Row(8L, false, false, false, false, true, "Bob", null, "US", null, null),
        Row(1014L, false, false, false, true, false, null, 454L, null, "Music", "Roland Kaiser"),
        Row(1015L, false, false, false, true, false, null, 743L, null, "Music", "Snap"),
        Row(26L, false, true, false, false, false, "Video", null, "EU", null, null),
        Row(3L, true, false, false, false, false, "Malmö", null, "EU", null, null),
        Row(9L, false, false, false, false, true, "Eve", null, "US", null, null),
        Row(4L, true, false, false, false, false, "Berlin", null, "EU", null, null),
        Row(1016L, false, false, false, true, false, null, 623L, null, "Music", "Dr.Alban"),
        Row(0L, true, false, false, false, false, "New York City", null, "US", null, null),
        Row(20L, false, true, false, false, false, "Book", null, "US", null, null),
        Row(2004L, false, false, true, false, false, "Carol", null, null, null, null),
        Row(1004L, false, false, false, true, false, null, 478L, null, "Book", "The Circle"),
        Row(2011L, false, false, true, false, false, "Victor", null, null, null, null),
        Row(2012L, false, false, true, false, false, "Peggy", null, null, null, null),
        Row(7L, false, false, false, false, true, "Alice", null, "US", null, null),
        Row(19L, false, false, false, false, true, "EuLoner", null, "EU", null, null),
        Row(1011L, false, false, false, true, false, null, 862L, null, "Video", "Sharknado"),
        Row(12L, false, false, false, false, true, "Dave", null, "US", null, null),
        Row(1012L, false, false, false, true, false, null, 347L, null, "Video", "Turtles"),
        Row(2001L, false, false, true, false, false, "Alice", null, null, null, null),
        Row(2008L, false, false, true, false, false, "Trudy", null, null, null, null),
        Row(11L, false, false, false, false, true, "Carl", null, "US", null, null),
        Row(1001L, false, false, false, true, false, null, 246L, null, "Book", "1984"),
        Row(21L, false, true, false, false, false, "DVD", null, "US", null, null),
        Row(16L, false, false, false, false, true, "Oscar", null, "EU", null, null),
        Row(2005L, false, false, true, false, false, "Carl", null, null, null, null),
        Row(24L, false, true, false, false, false, "Book", null, "EU", null, null),
        Row(2L, true, false, false, false, false, "Amsterdam", null, "EU", null, null),
        Row(2003L, false, false, true, false, false, "Eve", null, null, null, null),
        Row(27L, false, true, false, false, false, "Music", null, "EU", null, null),
        Row(14L, false, false, false, false, true, "Trudy", null, "EU", null, null),
        Row(22L, false, true, false, false, false, "Video", null, "US", null, null)
      ))

    graph.relationships("r").asInstanceOf[CAPSRecords].toDF().drop("r").collect().toBag should equal(
      Bag(
        Row(18L, "KNOWS", 16L, "EU", null, null, null),
        Row(18L, "CLOSE_TO", 16L, null, null, null, null),
        Row(13L, "CLOSE_TO", 15L, null, null, null, null),
        Row(2002L, "IS", 8L, null, null, null, null),
        Row(17L, "HAS_INTEREST", 26L, "EU", null, null, null),
        Row(9L, "HAS_INTEREST", 21L, "US", null, null, null),
        Row(2007L, "BOUGHT", 1006L, null, 5L, 8L, 10L),
        Row(2012L, "IS", 18L, null, null, null, null),
        Row(16L, "LIVES_IN", 4L, "EU", null, null, null),
        Row(17L, "LIVES_IN", 4L, "EU", null, null, null),
        Row(2010L, "IS", 16L, null, null, null, null),
        Row(2004L, "BOUGHT", 1013L, null, 5L, 8L, 10L),
        Row(7L, "CLOSE_TO", 9L, null, null, null, null),
        Row(2011L, "IS", 17L, null, null, null, null),
        Row(18L, "HAS_INTEREST", 27L, "EU", null, null, null),
        Row(2004L, "IS", 10L, null, null, null, null),
        Row(8L, "CLOSE_TO", 9L, null, null, null, null),
        Row(16L, "CLOSE_TO", 17L, null, null, null, null),
        Row(14L, "LIVES_IN", 3L, "EU", null, null, null),
        Row(2007L, "IS", 13L, null, null, null, null),
        Row(7L, "CLOSE_TO", 8L, null, null, null, null),
        Row(12L, "HAS_INTEREST", 23L, "US", null, null, null),
        Row(2001L, "BOUGHT", 1001L, null, 4L, 7L, 10L),
        Row(2001L, "BOUGHT", 1005L, null, 5L, 8L, 10L),
        Row(11L, "LIVES_IN", 1L, "US", null, null, null),
        Row(11L, "KNOWS", 12L, "US", null, null, null),
        Row(18L, "CLOSE_TO", 17L, null, null, null, null),
        Row(2001L, "IS", 7L, null, null, null, null),
        Row(7L, "KNOWS", 9L, "US", null, null, null),
        Row(14L, "KNOWS", 15L, "EU", null, null, null),
        Row(12L, "LIVES_IN", 1L, "US", null, null, null),
        Row(10L, "CLOSE_TO", 11L, null, null, null, null),
        Row(19L, "LIVES_IN", 2L, "EU", null, null, null),
        Row(11L, "CLOSE_TO", 12L, null, null, null, null),
        Row(5L, "LIVES_IN", 6L, "US", null, null, null),
        Row(10L, "CLOSE_TO", 12L, null, null, null, null),
        Row(7L, "KNOWS", 8L, "US", null, null, null),
        Row(13L, "CLOSE_TO", 14L, null, null, null, null),
        Row(9L, "HAS_INTEREST", 25L, "EU", null, null, null),
        Row(16L, "KNOWS", 17L, "EU", null, null, null),
        Row(2007L, "BOUGHT", 1002L, null, 4L, 7L, 10L),
        Row(15L, "LIVES_IN", 3L, "EU", null, null, null),
        Row(2009L, "IS", 15L, null, null, null, null),
        Row(10L, "LIVES_IN", 1L, "US", null, null, null),
        Row(2004L, "BOUGHT", 1009L, null, 4L, 7L, 10L),
        Row(13L, "LIVES_IN", 3L, "EU", null, null, null),
        Row(2008L, "IS", 14L, null, null, null, null),
        Row(2010L, "BOUGHT", 1003L, null, 4L, 7L, 10L),
        Row(8L, "LIVES_IN", 0L, "US", null, null, null),
        Row(14L, "HAS_INTEREST", 24L, "EU", null, null, null),
        Row(10L, "KNOWS", 11L, "US", null, null, null),
        Row(2010L, "BOUGHT", 1007L, null, 5L, 5L, 10L),
        Row(9L, "LIVES_IN", 0L, "US", null, null, null),
        Row(2005L, "IS", 11L, null, null, null, null),
        Row(7L, "LIVES_IN", 0L, "US", null, null, null),
        Row(2006L, "IS", 12L, null, null, null, null),
        Row(11L, "HAS_INTEREST", 22L, "US", null, null, null),
        Row(8L, "HAS_INTEREST", 20L, "US", null, null, null),
        Row(8L, "KNOWS", 9L, "US", null, null, null),
        Row(2003L, "IS", 9L, null, null, null, null),
        Row(18L, "LIVES_IN", 4L, "EU", null, null, null),
        Row(13L, "KNOWS", 14L, "EU", null, null, null),
        Row(14L, "CLOSE_TO", 15L, null, null, null, null)
      ))
  }

  def verifyLinks(graph: PropertyGraph) = {
    println("===>>> verifying LINKS")

    graph.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
        .withNodePropertyKeys("Customer")("name" -> CTString.nullable)
        .withRelationshipType("IN"))

    graph.nodes("n").collect.toBag should equal(
      Bag(
        Row(7L, true, false, "Alice", "US"),
        Row(2001L, false, true, "Alice", null),
        Row(8L, true, false, "Bob", "US"),
        Row(2002L, false, true, "Bob", null),
        Row(11L, true, false, "Carl", "US"),
        Row(2005L, false, true, "Carl", null),
        Row(10L, true, false, "Carol", "US"),
        Row(2004L, false, true, "Carol", null),
        Row(12L, true, false, "Dave", "US"),
        Row(2006L, false, true, "Dave", null),
        Row(9L, true, false, "Eve", "US"),
        Row(2003L, false, true, "Eve", null),
        Row(13L, true, false, "Mallory", "EU"),
        Row(2007L, false, true, "Mallory", null),
        Row(16L, true, false, "Oscar", "EU"),
        Row(2010L, false, true, "Oscar", null),
        Row(18L, true, false, "Peggy", "EU"),
        Row(2012L, false, true, "Peggy", null),
        Row(14L, true, false, "Trudy", "EU"),
        Row(2008L, false, true, "Trudy", null),
        Row(15L, true, false, "Trent", "EU"),
        Row(2009L, false, true, "Trent", null),
        Row(17L, true, false, "Victor", "EU"),
        Row(2011L, false, true, "Victor", null)
      ))

    val relsWithoutRelId = graph.relationships("r").asInstanceOf[CAPSRecords].toDF().drop("r")
    relsWithoutRelId.collect().toBag should equal(
      Bag(
        Row(2009L, "IS", 15L),
        Row(2011L, "IS", 17L),
        Row(2001L, "IS", 7L),
        Row(2002L, "IS", 8L),
        Row(2003L, "IS", 9L),
        Row(2010L, "IS", 16L),
        Row(2005L, "IS", 11L),
        Row(2012L, "IS", 18L),
        Row(2007L, "IS", 13L),
        Row(2006L, "IS", 12L),
        Row(2008L, "IS", 14L),
        Row(2004L, "IS", 10L)
      ))
  }

  def verifyCityFriendsUS(g: PropertyGraph): Assertion = {
    println("===>>> verifying CITYFRIENDS_US")

    g.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
        .withRelationshipType("CLOSE_TO"))

    g.nodes("n").collect.toBag should equal(
      Bag(
        Row(7L, true, "Alice", "US"),
        Row(8L, true, "Bob", "US"),
        Row(9L, true, "Eve", "US"),
        Row(10L, true, "Carol", "US"),
        Row(11L, true, "Carl", "US"),
        Row(12L, true, "Dave", "US")
      ))

    val relsWithoutRelId = g.relationships("r").asInstanceOf[CAPSRecords].toDF().drop("r")
    relsWithoutRelId.collect().toBag should equal(
      Bag(
        Row(7L, "CLOSE_TO", 8L),
        Row(7L, "CLOSE_TO", 9L),
        Row(8L, "CLOSE_TO", 9L),
        Row(10L, "CLOSE_TO", 11L),
        Row(10L, "CLOSE_TO", 12L),
        Row(11L, "CLOSE_TO", 12L)
      ))
  }

  def verifyCityFriendsEU(g: PropertyGraph): Assertion = {
    println("===>>> verifying CITYFRIENDS_EU")

    g.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
        .withRelationshipType("CLOSE_TO"))

    g.nodes("n").collect.toBag should equal(
      Bag(
        Row(13L, true, "Mallory", "EU"),
        Row(14L, true, "Trudy", "EU"),
        Row(15L, true, "Trent", "EU"),
        Row(16L, true, "Oscar", "EU"),
        Row(17L, true, "Victor", "EU"),
        Row(18L, true, "Peggy", "EU")
      ))

    val relsWithoutRelId = g.relationships("r").asInstanceOf[CAPSRecords].toDF().drop("r")
    relsWithoutRelId.collect().toBag should equal(
      Bag(
        Row(13L, "CLOSE_TO", 14L),
        Row(13L, "CLOSE_TO", 15L),
        Row(14L, "CLOSE_TO", 15L),
        Row(18L, "CLOSE_TO", 16L),
        Row(18L, "CLOSE_TO", 17L),
        Row(16L, "CLOSE_TO", 17L)
      ))
  }

  def verifyAllCityFriends(g: PropertyGraph): Assertion = {
    println("===>>> verifying ALL_CITY_FRIENDS")

    g.schema should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
        .withRelationshipType("CLOSE_TO"))

    g.nodes("n").collect.toBag should equal(
      Bag(
        Row(7L, true, "Alice", "US"),
        Row(8L, true, "Bob", "US"),
        Row(9L, true, "Eve", "US"),
        Row(10L, true, "Carol", "US"),
        Row(11L, true, "Carl", "US"),
        Row(12L, true, "Dave", "US"),
        Row(13L, true, "Mallory", "EU"),
        Row(14L, true, "Trudy", "EU"),
        Row(15L, true, "Trent", "EU"),
        Row(16L, true, "Oscar", "EU"),
        Row(17L, true, "Victor", "EU"),
        Row(18L, true, "Peggy", "EU")
      ))

    val relsWithoutRelId = g.relationships("r").asInstanceOf[CAPSRecords].toDF().drop("r")
    Bag(relsWithoutRelId.collect(): _*) should equal(
      Bag(
        Row(7L, "CLOSE_TO", 8L),
        Row(7L, "CLOSE_TO", 9L),
        Row(8L, "CLOSE_TO", 9L),
        Row(10L, "CLOSE_TO", 11L),
        Row(10L, "CLOSE_TO", 12L),
        Row(11L, "CLOSE_TO", 12L),
        Row(13L, "CLOSE_TO", 14L),
        Row(13L, "CLOSE_TO", 15L),
        Row(14L, "CLOSE_TO", 15L),
        Row(18L, "CLOSE_TO", 16L),
        Row(18L, "CLOSE_TO", 17L),
        Row(16L, "CLOSE_TO", 17L)
      ))
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
