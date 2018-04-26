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
package org.opencypher.spark.impl

import org.apache.spark.sql.{Row, functions}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.okapi.testing.propertygraph.TestGraphFactory
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

class CAPSScanGraphTest extends CAPSGraphTest {

  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  it("executes union") {
    val graph1 = CAPSGraph.create(personTable, knowsTable)
    val graph2 = CAPSGraph.create(programmerTable, bookTable, readsTable)

    val result = graph1 unionAll graph2

    val nodes = result.nodes("n").toDF().collect.toBag
    nodes should equal(
      Bag(
        Row(1L, false, true, false, true, null, 23L, "Mats", null, null),
        Row(2L, false, true, false, false, null, 42L, "Martin", null, null),
        Row(3L, false, true, false, false, null, 1337L, "Max", null, null),
        Row(4L, false, true, false, false, null, 9L, "Stefan", null, null),
        Row(10L.setTag(1), true, false, false, false, null, null, null, "1984", 1949L),
        Row(20L.setTag(1), true, false, false, false, null, null, null, "Cryptonomicon", 1999L),
        Row(30L.setTag(1), true, false, false, false, null, null, null, "The Eye of the World", 1990L),
        Row(40L.setTag(1), true, false, false, false, null, null, null, "The Circle", 2013L),
        Row(100L.setTag(1), false, true, true, false, "C", 42L, "Alice", null, null),
        Row(200L.setTag(1), false, true, true, false, "D", 23L, "Bob", null, null),
        Row(300L.setTag(1), false, true, true, false, "F", 84L, "Eve", null, null),
        Row(400L.setTag(1), false, true, true, false, "R", 49L, "Carl", null, null)
      )
    )

    val rels = result.relationships("r").toDF().collect.toBag
    rels should equal(
      Bag(
        Row(1L, 1L, "KNOWS", 2L, null, 2017L),
        Row(1L, 2L, "KNOWS", 3L, null, 2016L),
        Row(1L, 3L, "KNOWS", 4L, null, 2015L),
        Row(2L, 4L, "KNOWS", 3L, null, 2016L),
        Row(2L, 5L, "KNOWS", 4L, null, 2013L),
        Row(3L, 6L, "KNOWS", 4L, null, 2016L),
        Row(100L.setTag(1), 100L.setTag(1), "READS", 10L.setTag(1), true, null),
        Row(200L.setTag(1), 200L.setTag(1), "READS", 40L.setTag(1), true, null),
        Row(300L.setTag(1), 300L.setTag(1), "READS", 30L.setTag(1), true, null),
        Row(400L.setTag(1), 400L.setTag(1), "READS", 20L.setTag(1), false, null)
      )
    )
  }

  test("dont lose schema information when mapping") {
    val nodes = CAPSNodeTable(NodeMapping.on("id"),
      caps.sparkSession.createDataFrame(
        Seq(
          Tuple1(10L),
          Tuple1(11L),
          Tuple1(12L),
          Tuple1(20L),
          Tuple1(21L),
          Tuple1(22L),
          Tuple1(25L),
          Tuple1(50L),
          Tuple1(51L)
        )
      ).toDF("id"))

    val rs = CAPSRelationshipTable(RelationshipMapping.on("ID").from("SRC").to("DST").relType("FOO"),
      caps.sparkSession.createDataFrame(
        Seq(
          (10L, 1000L, 20L),
          (50L, 500L, 25L)
        )
      ).toDF("SRC", "ID", "DST"))


    val graph = CAPSGraph.create(nodes, rs)

    val results = graph.relationships("r").toCypherMaps

    results.collect().toSet should equal(
      Set(
        CypherMap("r" -> CAPSRelationship(1000L, 10L, 20L, "FOO")),
        CypherMap("r" -> CAPSRelationship(500L, 50L, 25L, "FOO"))
      ))
  }

  test("Construct graph from single node scan") {
    val graph = CAPSGraph.create(personTable)
    val nodes = graph.nodes("n")

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, true, true, 23L, "Mats"),
        Row(2L, true, false, 42L, "Martin"),
        Row(3L, true, false, 1337L, "Max"),
        Row(4L, true, false, 9L, "Stefan")))
  }

  test("Construct graph from multiple node scans") {
    val graph = CAPSGraph.create(personTable, bookTable)
    val nodes = graph.nodes("n")

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Book",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING",
        "____n_dot_titleSTRING",
        "____n_dot_yearINTEGER"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, false, true, true, 23L, "Mats", null, null),
        Row(2L, false, true, false, 42L, "Martin", null, null),
        Row(3L, false, true, false, 1337L, "Max", null, null),
        Row(4L, false, true, false, 9L, "Stefan", null, null),
        Row(10L, true, false, false, null, null, "1984", 1949L),
        Row(20L, true, false, false, null, null, "Cryptonomicon", 1999L),
        Row(30L, true, false, false, null, null, "The Eye of the World", 1990L),
        Row(40L, true, false, false, null, null, "The Circle", 2013L)
      ))
  }

  it("Align node scans") {
    val fixture =
      """
        |CREATE (a:A { name: 'A' })
        |CREATE (b:B { name: 'B' })
        |CREATE (combo:A:B { name: 'COMBO', size: 2 })
        |CREATE (a)-[:R { since: 2004 }]->(b)
        |CREATE (b)-[:R { since: 2005 }]->(combo)
        |CREATE (combo)-[:S { since: 2006 }]->(combo)
      """.stripMargin

    val graph = CAPSScanGraphFactory(TestGraphFactory(fixture))

    graph.cypher("MATCH (n) RETURN n").getRecords.size should equal(3)
  }

  it("Align node scans when individual tables have the same node id and properties") {
    val aDf = caps.sparkSession.createDataFrame(Seq(
      (0L, "A")
    )).toDF("_node_id", "name").withColumn("size", functions.lit(null))
    val aMapping: NodeMapping = NodeMapping.create("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabel("A")
    val aTable = CAPSNodeTable(aMapping, aDf)

    val bDf = caps.sparkSession.createDataFrame(Seq(
      (1L, "B")
    )).toDF("_node_id", "name").withColumn("size", functions.lit(null))
    val bMapping = NodeMapping.create("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabel("B")
    val bTable = CAPSNodeTable(bMapping, bDf)

    val comboDf = caps.sparkSession.createDataFrame(Seq(
      (2L, "COMBO", 2)
    )).toDF("_node_id", "name", "size")
    val comboMapping = NodeMapping.create("_node_id").withPropertyKey("name").withPropertyKey("size").withImpliedLabels("A", "B")
    val comboTable = CAPSNodeTable(comboMapping, comboDf)

    val graph = CAPSGraph.create(aTable, bTable, comboTable)

    graph.cypher("MATCH (n) RETURN n").getRecords.size should equal(3)
  }

  test("Construct graph from single node and single relationship scan") {
    val graph = CAPSGraph.create(personTable, knowsTable)
    val rels = graph.relationships("e")

    rels.toDF().columns should equal(
      Array(
        "____source(e)",
        "e",
        "____type(e)",
        "____target(e)",
        "____e_dot_sinceINTEGER"
      ))

    rels.toDF().collect().toSet should equal(
      Set(
        Row(1L, 1L, "KNOWS", 2L, 2017L),
        Row(1L, 2L, "KNOWS", 3L, 2016L),
        Row(1L, 3L, "KNOWS", 4L, 2015L),
        Row(2L, 4L, "KNOWS", 3L, 2016L),
        Row(2L, 5L, "KNOWS", 4L, 2013L),
        Row(3L, 6L, "KNOWS", 4L, 2016L)
      ))
  }

  test("Extract all node scans") {
    val graph = CAPSGraph.create(personTable, bookTable)

    val nodes = graph.nodes("n", CTNode())

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Book",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING",
        "____n_dot_titleSTRING",
        "____n_dot_yearINTEGER"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, false, true, true, 23L, "Mats", null, null),
        Row(2L, false, true, false, 42L, "Martin", null, null),
        Row(3L, false, true, false, 1337L, "Max", null, null),
        Row(4L, false, true, false, 9L, "Stefan", null, null),
        Row(10L, true, false, false, null, null, "1984", 1949L),
        Row(20L, true, false, false, null, null, "Cryptonomicon", 1999L),
        Row(30L, true, false, false, null, null, "The Eye of the World", 1990L),
        Row(40L, true, false, false, null, null, "The Circle", 2013L)
      ))
  }

  test("Extract node scan subset") {
    val graph = CAPSGraph.create(personTable, bookTable)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, true, true, 23L, "Mats"),
        Row(2L, true, false, 42L, "Martin"),
        Row(3L, true, false, 1337L, "Max"),
        Row(4L, true, false, 9L, "Stefan")))
  }

  test("Extract all relationship scans") {
    val graph = CAPSGraph.create(personTable, bookTable, knowsTable, readsTable)

    val rels = graph.relationships("e")

    rels.toDF().columns should equal(
      Array(
        "____source(e)",
        "e",
        "____type(e)",
        "____target(e)",
        "____e_dot_recommendsBOOLEAN",
        "____e_dot_sinceINTEGER"
      ))

    Bag(rels.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, 1L, "KNOWS", 2L, null, 2017L),
        Row(1L, 2L, "KNOWS", 3L, null, 2016L),
        Row(1L, 3L, "KNOWS", 4L, null, 2015L),
        Row(2L, 4L, "KNOWS", 3L, null, 2016L),
        Row(2L, 5L, "KNOWS", 4L, null, 2013L),
        Row(3L, 6L, "KNOWS", 4L, null, 2016L),
        Row(100L, 100L, "READS", 10L, true, null),
        Row(200L, 200L, "READS", 40L, true, null),
        Row(300L, 300L, "READS", 30L, true, null),
        Row(400L, 400L, "READS", 20L, false, null)
      ))
  }

  test("Extract relationship scan subset") {
    val graph = CAPSGraph.create(personTable, bookTable, knowsTable, readsTable)

    val rels = graph.relationships("e", CTRelationship("KNOWS"))

    rels.toDF().columns should equal(
      Array(
        "____source(e)",
        "e",
        "____type(e)",
        "____target(e)",
        "____e_dot_sinceINTEGER"
      ))

    rels.toDF().collect().toSet should equal(
      Set(
        Row(1L, 1L, "KNOWS", 2L, 2017L),
        Row(1L, 2L, "KNOWS", 3L, 2016L),
        Row(1L, 3L, "KNOWS", 4L, 2015L),
        Row(2L, 4L, "KNOWS", 3L, 2016L),
        Row(2L, 5L, "KNOWS", 4L, 2013L),
        Row(3L, 6L, "KNOWS", 4L, 2016L)
      ))
  }

  test("Extract relationship scan strict subset") {
    val graph = CAPSGraph.create(personTable, bookTable, knowsTable, readsTable, influencesTable)

    val rels = graph.relationships("e", CTRelationship("KNOWS", "INFLUENCES"))

    rels.toDF().columns should equal(
      Array(
        "____source(e)",
        "e",
        "____type(e)",
        "____target(e)",
        "____e_dot_sinceINTEGER"
      ))

    rels.toDF().collect().toSet should equal(
      Set(
        // :KNOWS
        Row(1L, 1L, "KNOWS", 2L, 2017L),
        Row(1L, 2L, "KNOWS", 3L, 2016L),
        Row(1L, 3L, "KNOWS", 4L, 2015L),
        Row(2L, 4L, "KNOWS", 3L, 2016L),
        Row(2L, 5L, "KNOWS", 4L, 2013L),
        Row(3L, 6L, "KNOWS", 4L, 2016L),
        // :INFLUENCES
        Row(10L, 1000L, "INFLUENCES", 20L, null)
      ))
  }

  test("Extract from scans with overlapping labels") {
    val graph = CAPSGraph.create(personTable, programmerTable)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n:Programmer",
        "____n:Swedish",
        "____n_dot_languageSTRING",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, true, false, true, null, 23L, "Mats"),
        Row(2L, true, false, false, null, 42L, "Martin"),
        Row(3L, true, false, false, null, 1337L, "Max"),
        Row(4L, true, false, false, null, 9L, "Stefan"),
        Row(100L, true, true, false, "C", 42L, "Alice"),
        Row(200L, true, true, false, "D", 23L, "Bob"),
        Row(300L, true, true, false, "F", 84L, "Eve"),
        Row(400L, true, true, false, "R", 49L, "Carl")
      ))
  }

  test("Extract from scans with implied label but missing keys") {
    val graph = CAPSGraph.create(personTable, brogrammerTable)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Brogrammer",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_languageSTRING",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    Bag(nodes.toDF().collect(): _*) should equal(
      Bag(
        Row(1L, false, true, true, null, 23L, "Mats"),
        Row(2L, false, true, false, null, 42L, "Martin"),
        Row(3L, false, true, false, null, 1337L, "Max"),
        Row(4L, false, true, false, null, 9L, "Stefan"),
        Row(100L, true, true, false, "Node", null, null),
        Row(200L, true, true, false, "Coffeescript", null, null),
        Row(300L, true, true, false, "Javascript", null, null),
        Row(400L, true, true, false, "Typescript", null, null)
      ))
  }
}
