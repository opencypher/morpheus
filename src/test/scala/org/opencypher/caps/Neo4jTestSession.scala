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
package org.opencypher.caps

import org.apache.spark.sql.Row
import org.neo4j.driver.v1.Config
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.opencypher.caps.api.io.neo4j.EncryptedNeo4jConfig
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTInteger, CTString}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

object Neo4jTestSession {

  trait AbstractFixture extends BeforeAndAfterAll {
    self: SparkTestSession.Fixture with FunSuite =>

    var neo4jServer: ServerControls = _

    def neo4jConfig = new EncryptedNeo4jConfig(neo4jServer.boltURI(),
      user = "anonymous",
      password = Some("password"),
      encryptionLevel = Config.EncryptionLevel.NONE)

    def neo4jHost: String = {
      val scheme = neo4jServer.boltURI().getScheme
      val userInfo = s"${neo4jConfig.user}:${neo4jConfig.password.get}@"
      val host = neo4jServer.boltURI().getAuthority
      s"$scheme://$userInfo$host"
    }

    def userFixture: String = "CALL dbms.security.createUser('anonymous', 'password', false)"

    def dataFixture: String

    override def beforeAll: Unit =
      neo4jServer = TestServerBuilders.newInProcessBuilder()
        .withConfig("dbms.security.auth_enabled", "true")
        .withFixture(userFixture)
        .withFixture(dataFixture)
        .newServer()

    override def afterAll: Unit = neo4jServer.close()
  }

  trait Fixture extends AbstractFixture {
    self: SparkTestSession.Fixture with FunSuite =>

    override val dataFixture =
      """
         CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
         CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
         CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
         CREATE (d:Person:German {name: "Max", luckyNumber: 8})
         CREATE (a)-[:KNOWS {since: 2016}]->(b)
         CREATE (b)-[:KNOWS {since: 2016}]->(c)
         CREATE (c)-[:KNOWS {since: 2016}]->(d)
      """

    def testGraphNodes: Set[Row] = Set(
      Row(0, true, true, false, 42, "Stefan"),
      Row(1, true, false, true, 23, "Mats"),
      Row(3, true, true, false, 8, "Max"),
      Row(2, true, true, false, 1337, "Martin")
    )

    def testGraphRels: Set[Row] = Set(
      Row(0, 0, 0, 1, 2016),
      Row(1, 1, 0, 2, 2016),
      Row(2, 2, 0, 3, 2016)
    )
  }

  trait OpenCypherFixture extends AbstractFixture {
    self: SparkTestSession.Fixture with FunSuite =>

    override val dataFixture =
      """CREATE (rachel:Person:Actor {name: 'Rachel Kempson', birthyear: 1910})
        |CREATE (michael:Person:Actor {name: 'Michael Redgrave', birthyear: 1908})
        |CREATE (vanessa:Person:Actor {name: 'Vanessa Redgrave', birthyear: 1937})
        |CREATE (corin:Person:Actor {name: 'Corin Redgrave', birthyear: 1939})
        |CREATE (liam:Person:Actor {name: 'Liam Neeson', birthyear: 1952})
        |CREATE (natasha:Person:Actor {name: 'Natasha Richardson', birthyear: 1963})
        |CREATE (richard:Person:Actor {name: 'Richard Harris', birthyear: 1930})
        |CREATE (dennis:Person:Actor {name: 'Dennis Quaid', birthyear: 1954})
        |CREATE (lindsay:Person:Actor {name: 'Lindsay Lohan', birthyear: 1986})
        |CREATE (jemma:Person:Actor {name: 'Jemma Redgrave', birthyear: 1965})
        |CREATE (roy:Person:Actor {name: 'Roy Redgrave', birthyear: 1873})
        |
        |CREATE (john:Person {name: 'John Williams', birthyear: 1932})
        |CREATE (christopher:Person {name: 'Christopher Nolan', birthyear: 1970})
        |
        |CREATE (newyork:City {name: 'New York'})
        |CREATE (london:City {name: 'London'})
        |CREATE (houston:City {name: 'Houston'})
        |
        |CREATE (mrchips:Film {title: 'Goodbye, Mr. Chips'})
        |CREATE (batmanbegins:Film {title: 'Batman Begins'})
        |CREATE (harrypotter:Film {title: 'Harry Potter and the Sorcerer\'s Stone'})
        |CREATE (parent:Film {title: 'The Parent Trap'})
        |CREATE (camelot:Film {title: 'Camelot'})
        |
        |CREATE (rachel)-[:HAS_CHILD]->(vanessa),
        |       (rachel)-[:HAS_CHILD]->(corin),
        |       (michael)-[:HAS_CHILD]->(vanessa),
        |       (michael)-[:HAS_CHILD]->(corin),
        |       (corin)-[:HAS_CHILD]->(jemma),
        |       (vanessa)-[:HAS_CHILD]->(natasha),
        |       (roy)-[:HAS_CHILD]->(michael),
        |
        |       (rachel)-[:MARRIED]->(michael),
        |       (michael)-[:MARRIED]->(rachel),
        |       (natasha)-[:MARRIED]->(liam),
        |       (liam)-[:MARRIED]->(natasha),
        |
        |       (vanessa)-[:BORN_IN]->(london),
        |       (natasha)-[:BORN_IN]->(london),
        |       (christopher)-[:BORN_IN]->(london),
        |       (dennis)-[:BORN_IN]->(houston),
        |       (lindsay)-[:BORN_IN]->(newyork),
        |       (john)-[:BORN_IN]->(newyork),
        |
        |       (christopher)-[:DIRECTED]->(batmanbegins),
        |
        |       (john)-[:WROTE_MUSIC_FOR]->(harrypotter),
        |       (john)-[:WROTE_MUSIC_FOR]->(mrchips),
        |
        |       (michael)-[:ACTED_IN {charactername: 'The Headmaster'}]->(mrchips),
        |       (vanessa)-[:ACTED_IN {charactername: 'Guenevere'}]->(camelot),
        |       (richard)-[:ACTED_IN {charactername: 'King Arthur'}]->(camelot),
        |       (richard)-[:ACTED_IN {charactername: 'Albus Dumbledore'}]->(harrypotter),
        |       (natasha)-[:ACTED_IN {charactername: 'Liz James'}]->(parent),
        |       (dennis)-[:ACTED_IN {charactername: 'Nick Parker'}]->(parent),
        |       (lindsay)-[:ACTED_IN {charactername: 'Halle/Annie'}]->(parent),
        |       (liam)-[:ACTED_IN {charactername: 'Henri Ducard'}]->(batmanbegins)
      """.stripMargin

    val nbrNodes = 21
    val nbrRels = 28

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString, "birthyear" -> CTInteger)
      // TODO: Investigate whether duplicating schema info like this is good
      .withNodePropertyKeys("Actor")("name" -> CTString, "birthyear" -> CTInteger)
      .withNodePropertyKeys("City")("name" -> CTString)
      .withNodePropertyKeys("Film")("title" -> CTString)
      // TODO: Calculate implied labels in schema construction .withImpliedLabel("Actor", "Person")
      .withRelationshipPropertyKeys("HAS_CHILD")()
      .withRelationshipPropertyKeys("MARRIED")()
      .withRelationshipPropertyKeys("BORN_IN")()
      .withRelationshipPropertyKeys("DIRECTED")()
      .withRelationshipPropertyKeys("WROTE_MUSIC_FOR")()
      .withRelationshipPropertyKeys("ACTED_IN")("charactername" -> CTString)
  }
}
