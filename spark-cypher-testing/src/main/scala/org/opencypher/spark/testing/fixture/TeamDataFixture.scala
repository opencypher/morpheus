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
package org.opencypher.spark.testing.fixture

import org.apache.spark.sql.{DataFrame, Row}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}

import scala.collection.mutable

trait TeamDataFixture extends TestDataFixture {

  self: CAPSSessionFixture =>

  val n: Var = Var("n")(CTNode)
  val nHasLabelGerman: Expr = HasLabel(n, Label("German"))
  val nHasLabelBook: Expr = HasLabel(n, Label("Book"))
  val nHasLabelPerson: Expr = HasLabel(n, Label("Person"))
  val nHasLabelProgrammer: Expr = HasLabel(n, Label("Programmer"))
  val nHasLabelBrogrammer: Expr = HasLabel(n, Label("Brogrammer"))
  val nHasPropertyLanguage: Expr = Property(n, PropertyKey("language"))(CTString)
  val nHasPropertyLuckyNumber: Expr = Property(n, PropertyKey("luckyNumber"))(CTInteger)
  val nHasPropertyTitle: Expr = Property(n, PropertyKey("title"))(CTString)
  val nHasPropertyYear: Expr = Property(n, PropertyKey("year"))(CTInteger)
  val nHasPropertyName: Expr = Property(n, PropertyKey("name"))(CTString)

  val r: Var = Var("r")(CTRelationship)
  val rStart: Expr = StartNode(r)(CTNode)
  val rEnd: Expr = EndNode(r)(CTNode)
  val rHasTypeKnows: Expr = HasType(r, RelType("KNOWS"))
  val rHasTypeReads: Expr = HasType(r, RelType("READS"))
  val rHasTypeInfluences: Expr = HasType(r, RelType("INFLUENCES"))
  val rHasPropertyRecommends: Expr = Property(r, PropertyKey("recommends"))(CTBoolean)
  val rHasPropertySince: Expr = Property(r, PropertyKey("since"))(CTInteger)

  override lazy val dataFixture =
    """
       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42, languages: ['German', 'English', 'Klingon']})
       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
       CREATE (e:Person {name: "Donald", luckyNumber: 8, languages: []})
       CREATE (a)-[:KNOWS {since: 2016}]->(b)
       CREATE (b)-[:KNOWS {since: 2016}]->(c)
       CREATE (c)-[:KNOWS {since: 2016}]->(d)
    """

  lazy val dataFixtureSchema: Schema = Schema.empty
    .withNodePropertyKeys("Person", "German")("name" -> CTString, "luckyNumber" -> CTInteger, "languages" -> CTList(CTString).nullable)
    .withNodePropertyKeys("Person", "Swede")("name" -> CTString, "luckyNumber" -> CTInteger)
    .withNodePropertyKeys("Person")("name" -> CTString, "luckyNumber" -> CTInteger, "languages" -> CTList(CTVoid))
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger)

  override lazy val nbrNodes = 4

  override def nbrRels = 3

  lazy val teamDataGraphNodes: Bag[CypherMap] = Bag(
    CypherMap("n" -> CAPSNode(0L, Set("Person", "German"), CypherMap("name" -> "Stefan", "luckyNumber" -> 42L, "languages" -> CypherList("German", "English", "Klingon")))),
    CypherMap("n" -> CAPSNode(1L, Set("Person", "Swede"), CypherMap("name" -> "Mats", "luckyNumber" -> 23L))),
    CypherMap("n" -> CAPSNode(2L, Set("Person", "German"), CypherMap("name" -> "Martin", "luckyNumber" -> 1337L))),
    CypherMap("n" -> CAPSNode(3L, Set("Person", "German"), CypherMap("name" -> "Max", "luckyNumber" -> 8L))),
    CypherMap("n" -> CAPSNode(4L, Set("Person"), CypherMap("name" -> "Donald", "luckyNumber" -> 8L, "languages" -> CypherList())))
  )

  lazy val teamDataGraphRels: Bag[CypherMap] = Bag(
    CypherMap("r" -> CAPSRelationship(0, 0, 1, "KNOWS", CypherMap("since" -> 2016))),
    CypherMap("r" -> CAPSRelationship(1, 1, 2, "KNOWS", CypherMap("since" -> 2016))),
    CypherMap("r" -> CAPSRelationship(2, 2, 3, "KNOWS", CypherMap("since" -> 2016)))
  )

  /**
    * Returns the expected graph tags for the test graph in /resources/csv/sn
    *
    * @return expected graph tags
    */
  lazy val csvTestGraphTags: Set[Int] = Set(0, 1)

  /**
    * Returns the expected nodes for the test graph in /resources/csv/sn
    *
    * @return expected nodes
    */
  lazy val csvTestGraphNodes: Bag[Row] = Bag(
    Row(1L, true, true, true, false, wrap(Array("german", "english")), 42L, "Stefan"),
    Row(2L, true, false, true, true, wrap(Array("swedish", "english", "german")), 23L, "Mats"),
    Row(3L, true, true, true, false, wrap(Array("german", "english")), 1337L, "Martin"),
    Row(4L, true, true, true, false, wrap(Array("german", "swedish", "english")), 8L, "Max")
  )

  // TODO: figure out why the column order is different for the calls in this and the next method
  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[org.opencypher.okapi.relational.api.graph.RelationalCypherGraph[DataFrameTable]#relationships]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRels: Bag[Row] = Bag(
    Row(1L, 10L, "KNOWS", 2L, 2016L),
    Row(2L, 20L, "KNOWS", 3L, 2017L),
    Row(3L, 30L, "KNOWS", 4L, 2015L)
  )

  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[[org.opencypher.okapi.relational.api.graph.RelationalCypherGraph[DataFrameTable]#records]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRelsFromRecords: Bag[Row] = Bag(
    Row(10L, 1L, "KNOWS", 2L, 2016L),
    Row(20L, 2L, "KNOWS", 3L, 2017L),
    Row(30L, 3L, "KNOWS", 4L, 2015L)
  )

  // TODO: remove once https://issues.apache.org/jira/browse/SPARK-23610 is resolved
  lazy val dataFixtureWithoutArrays =
    """
       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
       CREATE (e:Person {name: "Donald", luckyNumber: 8})
       CREATE (a)-[:KNOWS {since: 2015}]->(b)
       CREATE (b)-[:KNOWS {since: 2016}]->(c)
       CREATE (c)-[:KNOWS {since: 2017}]->(d)
    """

  lazy val csvTestGraphNodesWithoutArrays: Bag[Row] = Bag(
    Row(0L, true, true, false, 42L, "Stefan"),
    Row(1L, false, true, true, 23L, "Mats"),
    Row(2L, true, true, false, 1337L, "Martin"),
    Row(3L, true, true, false, 8L, "Max"),
    Row(4L, false, true, false, 8L, "Donald")
  )

  lazy val csvTestGraphRelsWithoutArrays: Bag[Row] = Bag(
    Row(0L, 5L, "KNOWS", 1L, 2015L),
    Row(1L, 6L, "KNOWS", 2L, 2016L),
    Row(2L, 7L, "KNOWS", 3L, 2017L)
  )

  private def wrap[T](s: Array[T]): mutable.WrappedArray[T] = {
    mutable.WrappedArray.make(s)
  }

  protected lazy val personMapping: EntityMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")
    .build

  protected lazy val personDF: DataFrame = caps.sparkSession.createDataFrame(
    Seq(
      (1L, "Mats", 23L),
      (2L, "Martin", 42L),
      (3L, "Max", 1337L),
      (4L, "Stefan", 9L))
  ).toDF("ID", "NAME", "NUM")

  lazy val personTable: CAPSEntityTable = CAPSEntityTable.create(personMapping, personDF)

  protected lazy val knowsMapping: EntityMapping = RelationshipMappingBuilder
    .on("ID").from("SRC")
    .to("DST")
    .relType("KNOWS")
    .withPropertyKey("since" -> "SINCE")
    .build


  protected lazy val knowsDF: DataFrame = caps.sparkSession.createDataFrame(
    Seq(
      (1L, 1L, 2L, 2017L),
      (1L, 2L, 3L, 2016L),
      (1L, 3L, 4L, 2015L),
      (2L, 4L, 3L, 2016L),
      (2L, 5L, 4L, 2013L),
      (3L, 6L, 4L, 2016L))
  ).toDF("SRC", "ID", "DST", "SINCE")

  lazy val knowsTable: CAPSEntityTable = CAPSEntityTable.create(knowsMapping, knowsDF)

  private lazy val programmerMapping: EntityMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Programmer")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")
    .withPropertyKey("language" -> "LANG")
    .build

  private lazy val programmerDF: DataFrame = caps.sparkSession.createDataFrame(
    Seq(
      (100L, "Alice", 42L, "C"),
      (200L, "Bob", 23L, "D"),
      (300L, "Eve", 84L, "F"),
      (400L, "Carl", 49L, "R")
    )).toDF("ID", "NAME", "NUM", "LANG")

  lazy val programmerTable: CAPSEntityTable= CAPSEntityTable.create(programmerMapping, programmerDF)

  private lazy val brogrammerMapping: EntityMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Brogrammer")
    .withImpliedLabel("Person")
    .withPropertyKey("language" -> "LANG")
    .build

  private lazy val brogrammerDF = caps.sparkSession.createDataFrame(
    Seq(
      (100L, "Node"),
      (200L, "Coffeescript"),
      (300L, "Javascript"),
      (400L, "Typescript")
    )).toDF("ID", "LANG")

  // required to test conflicting input data
  lazy val brogrammerTable: CAPSEntityTable = CAPSEntityTable.create(brogrammerMapping, brogrammerDF)

  private lazy val bookMapping: EntityMapping = NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Book")
    .withPropertyKey("title" -> "NAME")
    .withPropertyKey("year" -> "YEAR")
    .build

  private lazy val bookDF: DataFrame = caps.sparkSession.createDataFrame(
    Seq(
      (10L, "1984", 1949L),
      (20L, "Cryptonomicon", 1999L),
      (30L, "The Eye of the World", 1990L),
      (40L, "The Circle", 2013L)
    )).toDF("ID", "NAME", "YEAR")

  lazy val bookTable: CAPSEntityTable = CAPSEntityTable.create(bookMapping, bookDF)

  private lazy val readsMapping: EntityMapping = RelationshipMappingBuilder
    .on("ID").from("SRC").to("DST").relType("READS").withPropertyKey("recommends" -> "RECOMMENDS").build

  private lazy val readsDF = caps.sparkSession.createDataFrame(
    Seq(
      (100L, 100L, 10L, true),
      (200L, 200L, 40L, true),
      (300L, 300L, 30L, true),
      (400L, 400L, 20L, false)
    )).toDF("SRC", "ID", "DST", "RECOMMENDS")

  lazy val readsTable: CAPSEntityTable = CAPSEntityTable.create(readsMapping, readsDF)

  private lazy val influencesMapping: EntityMapping = RelationshipMappingBuilder
    .on("ID").from("SRC").to("DST").relType("INFLUENCES").build

  private lazy val influencesDF: DataFrame = caps.sparkSession.createDataFrame(
    Seq((10L, 1000L, 20L))).toDF("SRC", "ID", "DST")

  lazy val influencesTable: CAPSEntityTable = CAPSEntityTable.create(influencesMapping, influencesDF)
}
