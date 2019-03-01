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

import org.neo4j.harness.internal.InProcessNeo4j
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.testing.Neo4jUtils._
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp

/**
  * This application demonstrates the integration of three data sources into a single graph which is used for computing
  * recommendations. Two graphs are loaded from separate Neo4j databases, one graph is loaded from csv files stored in
  * the local file system.
  */
object RecommendationExample extends ConsoleApp {

  // Create CAPS session
  implicit val caps: CAPSSession = CAPSSession.local()

  // Start two Neo4j instances and populate them with social network data
  val neo4jServerUS: InProcessNeo4j = startNeo4j(socialNetworkUS)
  val neo4jServerEU: InProcessNeo4j = startNeo4j(socialNetworkEU)

  // Register Property Graph Data Sources (PGDS)

  // The graph within Neo4j is partitioned into regions using a property key. Within the data source, we map each
  // partition to a separate graph name (i.e. US and EU)
  caps.registerSource(Namespace("usSocialNetwork"), GraphSources.cypher.neo4j(neo4jServerUS.dataSourceConfig))
  caps.registerSource(Namespace("euSocialNetwork"), GraphSources.cypher.neo4j(neo4jServerEU.dataSourceConfig))

  // File-based CSV GDS
  caps.registerSource(Namespace("purchases"), GraphSources.fs(rootPath = s"${getClass.getResource("/fs-graphsource/csv").getFile}").csv)

  // Start analytical workload

  /**
    * Returns a query that creates a graph containing persons that live in the same city and
    * know each other via 1 to 2 hops. The created graph contains a CLOSE_TO relationship between
    * each such pair of persons and is stored in the session catalog using the given graph name.
    */
  def cityFriendsQuery(fromGraph: String): String =
    s"""FROM GRAPH $fromGraph
       |MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person),
       |      (a)-[:KNOWS*1..2]->(b)
       |CONSTRUCT
       |  ON $fromGraph
       |  CREATE (a)-[:CLOSE_TO]->(b)
       |RETURN GRAPH
      """.stripMargin

  // Find persons that are close to each other in the US social network
  val usFriends = caps.cypher(cityFriendsQuery(s"usSocialNetwork.$entireGraphName")).graph
  // Find persons that are close to each other in the EU social network
  val euFriends = caps.cypher(cityFriendsQuery(s"euSocialNetwork.$entireGraphName")).graph

  // Union the US and EU graphs into a single graph 'allFriends' and store it in the session
  caps.catalog.store("allFriends", usFriends.unionAll(euFriends))

  // Connect the social network with the products network using equal person and customer emails
  val connectedCustomers = caps.cypher(
    s"""FROM GRAPH allFriends
       |MATCH (p:Person)
       |FROM GRAPH purchases.products
       |MATCH (c:Customer)
       |WHERE c.name = p.name
       |CONSTRUCT ON purchases.products, allFriends
       |  CREATE (c)-[:IS]->(p)
       |RETURN GRAPH
      """.stripMargin).graph

  // Compute recommendations for 'target' based on their interests and what persons close to the
  // 'target' have already bought and given a helpful and positive rating
  val recommendationTable = connectedCustomers.cypher(
    s"""|MATCH (target:Person)<-[:CLOSE_TO]-(person:Person),
        |      (target)-[:HAS_INTEREST]->(i:Interest),
        |      (person)<-[:IS]-(x:Customer)-[b:BOUGHT]->(product:Product {category: i.name})
        |WHERE b.rating >= 4 AND (b.helpful * 1.0) / b.votes > 0.6
        |WITH * ORDER BY product.rank
        |RETURN DISTINCT product.title AS product, target.name AS name
        |LIMIT 3
      """.stripMargin).records

  // Print the results
  recommendationTable.show

  // Shutdown Neo4j test instance
  neo4jServerUS.stop()
  neo4jServerEU.stop()


  def socialNetworkUS =
    """
       CREATE (nyc:City {name: "New York City"})
       CREATE (sfo:City {name: "San Francisco"})

       CREATE (alice:Person   {name: "Alice"}  )-[:LIVES_IN]->(nyc)
       CREATE (bob:Person     {name: "Bob"}    )-[:LIVES_IN]->(nyc)
       CREATE (eve:Person     {name: "Eve"}    )-[:LIVES_IN]->(nyc)
       CREATE (carol:Person   {name: "Carol"}  )-[:LIVES_IN]->(sfo)
       CREATE (carl:Person    {name: "Carl"}   )-[:LIVES_IN]->(sfo)
       CREATE (dave:Person    {name: "Dave"}   )-[:LIVES_IN]->(sfo)

       CREATE (eve)<-[:KNOWS]-(alice)-[:KNOWS]->(bob)-[:KNOWS]->(eve)
       CREATE (carol)-[:KNOWS]->(carl)-[:KNOWS]->(dave)

       CREATE (book_US:Interest {name: "Book"})
       CREATE (dvd_US:Interest {name: "DVD"})
       CREATE (video_US:Interest {name: "Video"})
       CREATE (music_US:Interest {name: "Music"})

       CREATE (bob)-[:HAS_INTEREST]->(book_US)
       CREATE (eve)-[:HAS_INTEREST]->(dvd_US)
       CREATE (carl)-[:HAS_INTEREST]->(video_US)
       CREATE (dave)-[:HAS_INTEREST]->(music_US)
    """

  def socialNetworkEU =
    """
       CREATE (mal:City {name: "Malmö"})
       CREATE (ber:City {name: "Berlin"})

       CREATE (mallory:Person {name: "Mallory"})-[:LIVES_IN]->(mal)
       CREATE (trudy:Person   {name: "Trudy"}  )-[:LIVES_IN]->(mal)
       CREATE (trent:Person   {name: "Trent"}  )-[:LIVES_IN]->(mal)
       CREATE (oscar:Person   {name: "Oscar"}  )-[:LIVES_IN]->(ber)
       CREATE (victor:Person  {name: "Victor"} )-[:LIVES_IN]->(ber)
       CREATE (peggy:Person   {name: "Peggy"}  )-[:LIVES_IN]->(ber)

       CREATE (mallory)-[:KNOWS]->(trudy)-[:KNOWS]->(trent)
       CREATE (peggy)-[:KNOWS]->(oscar)-[:KNOWS]->(victor)

       CREATE (book_EU:Interest {name: "Book"})
       CREATE (dvd_EU:Interest {name: "DVD"})
       CREATE (video_EU:Interest {name: "Video"})
       CREATE (music_EU:Interest {name: "Music"})

       CREATE (trudy)-[:HAS_INTEREST]->(book_EU)
       CREATE (eve)-[:HAS_INTEREST]->(dvd_EU)
       CREATE (victor)-[:HAS_INTEREST]->(video_EU)
       CREATE (peggy)-[:HAS_INTEREST]->(music_EU)
    """

}
// end::full-example[]
