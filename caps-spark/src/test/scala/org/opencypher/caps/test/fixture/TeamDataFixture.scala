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
package org.opencypher.caps.test.fixture

import org.apache.spark.sql.Row
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.{NodeTable, RelationshipTable}
import org.opencypher.caps.impl.spark.CAPSRecords
import org.opencypher.caps.test.support.DebugOutputSupport

import scala.collection.Bag

trait TeamDataFixture extends TestDataFixture with DebugOutputSupport {

  self: CAPSSessionFixture =>

  override lazy val dataFixture =
    """
       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
       CREATE (a)-[:KNOWS {since: 2016}]->(b)
       CREATE (b)-[:KNOWS {since: 2016}]->(c)
       CREATE (c)-[:KNOWS {since: 2016}]->(d)
    """

  override lazy val nbrNodes = 4

  override def nbrRels = 3

  lazy val teamDataGraphNodes: Bag[Row] = Bag(
    Row(0L, true, true, false, 42L, "Stefan"),
    Row(1L, false, true, true, 23L, "Mats"),
    Row(2L, true, true, false, 1337L, "Martin"),
    Row(3L, true, true, false, 8L, "Max")
  )

  lazy val teamDataGraphRels: Bag[Row] = Bag(
    Row(0L, 0L, "KNOWS", 1L, 2016L),
    Row(1L, 1L, "KNOWS", 2L, 2016L),
    Row(2L, 2L, "KNOWS", 3L, 2016L)
  )

  private lazy val personMapping: NodeMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Person")
    .withOptionalLabel("Swedish" -> "IS_SWEDE")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("lucky_number" -> "NUM")

  private lazy val personRecords: CAPSRecords = CAPSRecords.create(
    Seq("ID", "IS_SWEDE", "NAME", "NUM"),
    Seq(
      (1L, true, "Mats", 23L),
      (2L, false, "Martin", 42L),
      (3L, false, "Max", 1337L),
      (4L, false, "Stefan", 9L))
  )
  
  lazy val personTable = NodeTable(personMapping, personRecords)

  private lazy val knowsMapping: RelationshipMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("KNOWS").withPropertyKey("since" -> "SINCE")


  private lazy val knowsRecords: CAPSRecords = CAPSRecords.create(
    Seq("SRC", "ID", "DST", "SINCE"),
    Seq(
      (1L, 1L, 2L, 2017L),
      (1L, 2L, 3L, 2016L),
      (1L, 3L, 4L, 2015L),
      (2L, 4L, 3L, 2016L),
      (2L, 5L, 4L, 2013L),
      (3L, 6L, 4L, 2016L))
  )
  lazy val knowsTable = RelationshipTable(knowsMapping, knowsRecords)

  private lazy val programmerMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Programmer")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("lucky_number" -> "NUM")
    .withPropertyKey("language" -> "LANG")

  private lazy val programmerRecords = CAPSRecords.create(
    Seq("ID", "NAME", "NUM", "LANG"),
    Seq(
      (100L, "Alice", 42L, "C"),
      (200L, "Bob", 23L, "D"),
      (300L, "Eve", 84L, "F"),
      (400L, "Carl", 49L, "R")
    )
  )

  lazy val programmerTable = NodeTable(programmerMapping, programmerRecords)


  private lazy val brogrammerMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Brogrammer")
    .withImpliedLabel("Person")
    .withPropertyKey("language" -> "LANG")

  private lazy val brogrammerRecords = CAPSRecords.create(
    Seq("ID", "LANG"),
    Seq(
      (100L, "Node"),
      (200L, "Coffeescript"),
      (300L, "Javascript"),
      (400L, "Typescript")
    )
  )

  // required to test conflicting input data
  lazy val brogrammerTable = NodeTable(brogrammerMapping, brogrammerRecords)

  private lazy val bookMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Book")
    .withPropertyKey("title" -> "NAME")
    .withPropertyKey("year" -> "YEAR")

  private lazy val bookRecords = CAPSRecords.create(
    Seq("ID", "NAME", "YEAR"),
    Seq(
      (10L, "1984", 1949L),
      (20L, "Cryptonomicon", 1999L),
      (30L, "The Eye of the World", 1990L),
      (40L, "The Circle", 2013L))
  )

  lazy val bookTable = NodeTable(bookMapping, bookRecords)


  private lazy val readsMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("READS").withPropertyKey("recommends" -> "RECOMMENDS")

  private lazy val readsRecords = CAPSRecords.create(
    Seq("SRC", "ID", "DST", "RECOMMENDS"),
    Seq(
      (100L, 100L, 10L, true),
      (200L, 200L, 40L, true),
      (300L, 300L, 30L, true),
      (400L, 400L, 20L, false))
  )

  lazy val readsTable = RelationshipTable(readsMapping, readsRecords)

  private lazy val influencesMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("INFLUENCES")

  private lazy val influencesRecords = CAPSRecords.create(
    Seq("SRC", "ID", "DST"),
    Seq((10L, 1000L, 20L))
  )

  lazy val influencesTable = RelationshipTable(influencesMapping, influencesRecords)
}
