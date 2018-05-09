/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
/**
  * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.opencypher.spark.testing.fixture

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

trait OpenCypherDataFixture extends TestDataFixture {

  override val dataFixture: String =
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
      |// non-standard nodes here, for testing schema handling
      |//CREATE (:Movie {title: 444}) not supported for unlabeled scans
      |CREATE (:Person {name: 'Fake Foo', birthyear: 1970})
      |CREATE (:Person {name: 'Fake Bar', birthyear: 1970})
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

  val nbrNodes = 23

  val nbrRels = 28

  val schema: CAPSSchema = Schema.empty
    .withNodePropertyKeys("Person")("name" -> CTString, "birthyear" -> CTInteger)
    .withNodePropertyKeys("Person", "Actor")("name" -> CTString, "birthyear" -> CTInteger)
    .withNodePropertyKeys("City")("name" -> CTString)
    .withNodePropertyKeys("Film")("title" -> CTString)
//    .withNodePropertyKeys("Movie")("title" -> CTInteger)
    .withRelationshipPropertyKeys("HAS_CHILD")()
    .withRelationshipPropertyKeys("MARRIED")()
    .withRelationshipPropertyKeys("BORN_IN")()
    .withRelationshipPropertyKeys("DIRECTED")()
    .withRelationshipPropertyKeys("WROTE_MUSIC_FOR")()
    .withRelationshipPropertyKeys("ACTED_IN")("charactername" -> CTString)
    .asCaps
}

