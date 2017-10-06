/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.demo

import java.net.{URI, URLEncoder}

import org.apache.spark.sql.Row
import org.neo4j.driver.v1.Session
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{MiniDFSClusterFixture, Neo4jServerFixture, SparkSessionFixture}

import scala.collection.JavaConversions._

class GCDemoTest
  extends BaseTestSuite
    with SparkSessionFixture
    with Neo4jServerFixture
    with MiniDFSClusterFixture
{

  implicit val caps: CAPSSession = CAPSSession.create(session)
  protected override val dfsTestGraphPath = "/csv/prod"

  ignore("the demo") {
    val SN_US = caps.graphAt(neoURIforRegion("US"))
    val SN_EU = caps.graphAt(neoURIforRegion("EU"))
    val PRODUCTS = caps.graphAt(hdfsURI)

    // Using GRAPH OF
    val CITYFRIENDS_US = SN_US.cypher(
      """MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
        |WHERE city.name = "New York City" OR city.name = "San Francisco"
        |RETURN GRAPH result OF (a)-[r:ACQUAINTED]->(b)
      """.stripMargin)
    verifyCityFriendsUS(CITYFRIENDS_US.graphs("result"))

    val CITYFRIENDS_EU = SN_EU.cypher(
      """MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person), (a)-[:KNOWS*1..2]->(b)
        |WHERE city.name = "Malmö" OR city.name = "Berlin"
        |RETURN GRAPH result OF (a)-[r:ACQUAINTED]->(b)
      """.stripMargin)

    val ALL_CITYFRIENDS = CITYFRIENDS_EU.graphs("result") union CITYFRIENDS_US.graphs("result")
    verifyFriendsUnion(ALL_CITYFRIENDS)

    caps.persistGraphAt(ALL_CITYFRIENDS, "/friends")

    val LINKS = caps.cypher(
      s"""FROM GRAPH friends AT '/friends'
         |MATCH (p:Person)
         |WITH p.name AS personName
         |FROM GRAPH products AT '$hdfsURI'
         |MATCH (c:Customer) WHERE c.name = personName
         |RETURN GRAPH result OF (c)-[x:IS]->(p)
      """.stripMargin)

    val RECO = ALL_CITYFRIENDS union PRODUCTS union LINKS.graphs("result")

    val result = RECO.cypher(
      """MATCH (a:Person)-[:ACQUAINTED]-(b:Person)-[:HAS_INTEREST]->(i:Interest),
        |      (a)<-[:IS]-(x:Customer)-[r:BOUGHT]->(p:Product {category: i.name})
        |WHERE r.rating >= 4 AND r.helpful / r.votes > 0.6
        |WITH * ORDER BY p.rank
        |RETURN DISTINCT p.title AS product, b.name AS name
        |LIMIT 100
      """.stripMargin)

    // Write back to Neo
    withBoltSession { session =>
      result.records.data.toLocalIterator().toIterator.foreach { row =>
        session.run(s"MATCH (p:Person {name: ${row.getString(1)}) SET p.should_buy = ${row.getString(0)}")
      }
    }
  }

  private def withBoltSession[T](f: Session => T): T = {
    val driver = org.neo4j.driver.v1.GraphDatabase.driver(neo4jHost)
    val session = driver.session()
    try {
      f(session)
    }
    finally {
      session.close()
    }
  }

  private def neoURIforRegion(region: String) = {
    val nodeQuery = URLEncoder.encode(s"MATCH (n {region: '$region'}) RETURN n", "UTF-8")
    val relQuery = URLEncoder.encode(s"MATCH ()-[r {region: '$region'}]->() RETURN r", "UTF-8")
    val uri = URI.create(s"$neo4jHost?$nodeQuery;$relQuery")
    uri
  }

  def verifyCityFriendsUS(g: CAPSGraph) = {
    g.nodes("n").details.toDF().collect().toSet should equal (Set(
      Row(4L,false,true,false,"Alice","US"),
      Row(5L,false,true,false,"Bob","US"),
      Row(6L,false,true,false,"Eve","US"),
      Row(7L,false,true,false,"Carol","US"),
      Row(8L,false,true,false,"Carl","US"),
      Row(9L,false,true,false,"Dave","US")
    ))

    val relsWithoutRelId = g.relationships("r").details.toDF().drop("r")

    relsWithoutRelId.collect().toSet should equal (Set(
      Row(4L, "ACQUAINTED", 6L),
      Row(7L, "ACQUAINTED", 8L),
      Row(5L, "ACQUAINTED", 6L),
      Row(4L, "ACQUAINTED", 5L),
      Row(8L, "ACQUAINTED", 9L),
      Row(7L, "ACQUAINTED", 9L)
    ))
  }

  def verifyFriendsUnion(g: CAPSGraph) = {
    g.nodes("n").details.toDF().collect().toSet should equal (Set(
      Row(4L,false,true,false,"Alice","US"),
      Row(5L,false,true,false,"Bob","US"),
      Row(6L,false,true,false,"Eve","US"),
      Row(7L,false,true,false,"Carol","US"),
      Row(8L,false,true,false,"Carl","US"),
      Row(9L,false,true,false,"Dave","US"),
      Row(10L,false,true,false,"Mallory","EU"),
      Row(11L,false,true,false,"Trudy","EU"),
      Row(12L,false,true,false,"Trent","EU"),
      Row(13L,false,true,false,"Oscar","EU"),
      Row(14L,false,true,false,"Victor","EU"),
      Row(15L,false,true,false,"Peggy","EU")
    ))

    val relsWithoutRelId = g.relationships("r").details.toDF().drop("r")

    relsWithoutRelId.collect().toSet should equal (Set(
      Row(4L, "ACQUAINTED", 6L),
      Row(7L, "ACQUAINTED", 8L),
      Row(5L, "ACQUAINTED", 6L),
      Row(4L, "ACQUAINTED", 5L),
      Row(8L, "ACQUAINTED", 9L),
      Row(7L, "ACQUAINTED", 9L),
      Row(8L, "ACQUAINTED", 9L),
      Row(10L, "ACQUAINTED", 11L),
      Row(10L, "ACQUAINTED", 12L),
      Row(11L, "ACQUAINTED", 12L),
      Row(13L, "ACQUAINTED", 14L),
      Row(13L, "ACQUAINTED", 15L),
      Row(14L, "ACQUAINTED", 15L)
    ))
  }

  override def dataFixture = """
       CREATE (nyc:City {name: "New York City", region: "US"})
       CREATE (sfo:City {name: "San Francisco", region: "US"})
       CREATE (mal:City {name: "Malmö", region: "EU"})
       CREATE (ber:City {name: "Berlin", region: "EU"})

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

       CREATE (alice)-[:KNOWS {region: "US"}]->(bob)-[:KNOWS {region: "US"}]->(eve)
       CREATE (carol)-[:KNOWS {region: "US"}]->(carl)-[:KNOWS {region: "US"}]->(dave)
       CREATE (mallory)-[:KNOWS {region: "EU"}]->(trudy)-[:KNOWS {region: "EU"}]->(trent)
       CREATE (oscar)-[:KNOWS {region: "EU"}]->(victor)-[:KNOWS {region: "EU"}]->(peggy)

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
