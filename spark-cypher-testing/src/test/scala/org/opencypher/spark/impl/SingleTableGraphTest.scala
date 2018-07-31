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

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.planning.InnerJoin
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.fixture.RecordsVerificationFixture
import org.opencypher.spark.testing.support.creation.caps.{CAPSTestGraphFactory, SingleTableGraphFactory}

import scala.collection.JavaConverters._

class SingleTableGraphTest extends CAPSGraphTest with RecordsVerificationFixture {

  import CAPSGraphTestData._

  override def capsGraphFactory: CAPSTestGraphFactory = SingleTableGraphFactory

  it("projects a pattern graph") {
    val inputGraph = initGraph(`:Person`)

    val person = inputGraph.cypher(
      """MATCH (a :Swedish)
        |CONSTRUCT
        |  CLONE a
        |RETURN GRAPH
      """.stripMargin)

    person.graph.cypher("MATCH (n) RETURN n.name").records.collect.toSet should equal(
      Set(
        CypherMap("n.name" -> "Mats")
      ))
  }

  it("projects a pattern graph with a relationship") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  CLONE a, b, r
        |  NEW (a)-[r]->(b)
        |RETURN GRAPH
      """.stripMargin)

    person.graph.cypher("MATCH (n) RETURN n.name").records.collect.toSet should equal(
      Set(
        CypherMap("n.name" -> "Mats"),
        CypherMap("n.name" -> "Stefan"),
        CypherMap("n.name" -> "Martin"),
        CypherMap("n.name" -> "Max")
      ))
  }

  it("projects a pattern graph with a created relationship") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  CLONE a, b
        |  NEW (a)-[foo:SWEDISH_KNOWS]->(b)
        |RETURN GRAPH
      """.stripMargin)

    person
      .graph
      .cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n.name")
      .records
      .collect
      .toSet should equal(
      Set(
        CypherMap("n.name" -> "Stefan"),
        CypherMap("n.name" -> "Martin"),
        CypherMap("n.name" -> "Max")
      ))
  }

  it("implictly clones when projecting a pattern graph with a created relationship") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  NEW (a)-[foo:SWEDISH_KNOWS]->(b)
        |RETURN GRAPH
      """.stripMargin)

    person
      .graph
      .cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n.name")
      .records
      .collect
      .toSet should equal(
      Set(
        CypherMap("n.name" -> "Stefan"),
        CypherMap("n.name" -> "Martin"),
        CypherMap("n.name" -> "Max")
      ))
  }

  it("projects a pattern graph with a created node") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  CLONE a, r, b
        |  NEW (a)-[r]->(b)-[:KNOWS_A]->()
        |RETURN GRAPH
      """.stripMargin)

    person
      .graph
      .cypher("MATCH (b)-[:KNOWS_A]->(n) WITH COUNT(n) as cnt RETURN cnt")
      .records
      .collect
      .toSet should equal(
      Set(
        CypherMap("cnt" -> 3)
      ))
  }

  it("implictly clones when projecting a pattern graph with a created node") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  NEW (a)-[r]->(b)-[:KNOWS_A]->()
        |RETURN GRAPH
      """.stripMargin)

    person
      .graph
      .cypher("MATCH (b)-[:KNOWS_A]->(n) WITH COUNT(n) as cnt RETURN cnt")
      .records
      .collect
      .toSet should equal(
      Set(
        CypherMap("cnt" -> 3)
      ))
  }

  it("relationship scan for specific type") {
    val inputGraph = initGraph(`:KNOWS` + `:READS`)
    val inputRels = inputGraph.relationships("r")

    val singleTableGraph = caps.graphs.create(inputRels, inputGraph.schema.asCaps)
    val outputRels = singleTableGraph.relationships("r", CTRelationship("KNOWS"))

    outputRels.table.df.count() shouldBe 6
  }

  it("relationship scan for disjunction of types") {
    val inputGraph = initGraph(`:KNOWS` + `:READS` + `:INFLUENCES`)
    val inputRels = inputGraph.relationships("r")

    val singleTableGraph = caps.graphs.create(inputRels, inputGraph.schema.asCaps)
    val outputRels = singleTableGraph.relationships("r", CTRelationship("KNOWS", "INFLUENCES"))

    outputRels.table.df.count() shouldBe 7
  }

  it("relationship scan for all types") {
    val inputGraph = initGraph(`:KNOWS` + `:READS`)
    val inputRels = inputGraph.relationships("r")

    val singleTableGraph = caps.graphs.singleTableGraph(inputRels, inputGraph.schema, Set(0))
    val outputRels = singleTableGraph.relationships("r", CTRelationship)

    outputRels.table.df.count() shouldBe 10
  }

  it("projects a pattern graph with a created node that has labels") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  CLONE a, r, b
        |  NEW (a)-[r]->(b)-[bar:KNOWS_A]->(baz:Swede)
        |RETURN GRAPH
      """.stripMargin)

    val graph = person.graph

    graph.cypher("MATCH (n:Swede) RETURN labels(n)").records.collect.toSet should equal(
      Set(
        CypherMap("labels(n)" -> List("Swede")),
        CypherMap("labels(n)" -> List("Swede")),
        CypherMap("labels(n)" -> List("Swede"))
      ))
  }

  it("implicitly clones when projecting a pattern graph with a created node that has labels") {
    val inputGraph = initGraph(`:Person` + `:KNOWS`)

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |CONSTRUCT
        |  NEW (a)-[r]->(b)-[bar:KNOWS_A]->(baz:Swede)
        |RETURN GRAPH
      """.stripMargin)

    val graph = person.graph

    graph.cypher("MATCH (n:Swede) RETURN labels(n)").records.collect.toSet should equal(
      Set(
        CypherMap("labels(n)" -> List("Swede")),
        CypherMap("labels(n)" -> List("Swede")),
        CypherMap("labels(n)" -> List("Swede"))
      ))
  }

  //TODO: Test creating literal property value
  //TODO: Test creating computed property value

  it("Node scan from single node CAPSRecords") {
    val inputGraph = initGraph(`:Person`)
    val inputNodes = inputGraph.nodes("n")

    val singleTableGraph = caps.graphs.singleTableGraph(inputNodes, inputGraph.schema, Set(0))
    val outputNodes = singleTableGraph.nodes("n")

    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasLabelSwedish,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row(0L, true, true, 23L, "Mats"),
      Row(1L, true, false, 42L, "Martin"),
      Row(2L, true, false, 1337L, "Max"),
      Row(3L, true, false, 9L, "Stefan")
    )

    verify(outputNodes, cols, data)
  }

  it("Node scan from mixed node CapsRecords") {
    val inputGraph = initGraph(`:Person` + `:Book`)
    val inputNodes = inputGraph.nodes("n")

    val singleTableGraph = caps.graphs.singleTableGraph(inputNodes, inputGraph.schema, Set(0))
    val outputNodes = singleTableGraph.nodes("n")

    val col = Seq(
      n,
      nHasLabelBook,
      nHasLabelPerson,
      nHasLabelSwedish,
      nHasPropertyLuckyNumber,
      nHasPropertyName,
      nHasPropertyTitle,
      nHasPropertyYear
    )
    val data = Bag(
      Row(0L, false, true, true, 23L, "Mats", null, null),
      Row(1L, false, true, false, 42L, "Martin", null, null),
      Row(2L, false, true, false, 1337L, "Max", null, null),
      Row(3L, false, true, false, 9L, "Stefan", null, null),
      Row(4L, true, false, false, null, null, "1984", 1949L),
      Row(5L, true, false, false, null, null, "Cryptonomicon", 1999L),
      Row(6L, true, false, false, null, null, "The Eye of the World", 1990L),
      Row(7L, true, false, false, null, null, "The Circle", 2013L)
    )

    verify(outputNodes, col, data)
  }

  it("Node scan from multiple connected nodes") {
    val singleTableGraph = initPersonReadsBookGraph
    val nodes = singleTableGraph.nodes("n")
    val cols = Seq(
      n,
      nHasLabelBook,
      nHasLabelPerson,
      nHasLabelSwedish,
      nHasPropertyLuckyNumber,
      nHasPropertyName,
      nHasPropertyTitle,
      nHasPropertyYear
    )
    val data = Bag(
      Row(0L, false, true, true, 23L, "Mats", null, null),
      Row(1L, false, true, false, 42L, "Martin", null, null),
      Row(2L, false, true, false, 1337L, "Max", null, null),
      Row(3L, false, true, false, 9L, "Stefan", null, null),
      Row(4L, true, false, false, null, null, "1984", 1949L),
      Row(5L, true, false, false, null, null, "Cryptonomicon", 1999L),
      Row(6L, true, false, false, null, null, "The Eye of the World", 1990L),
      Row(7L, true, false, false, null, null, "The Circle", 2013L)
    )

    verify(nodes, cols, data)
  }

  it("Specific node scan from multiple connected nodes") {
    val singleTableGraph = initPersonReadsBookGraph
    val nodes = singleTableGraph.nodes("n", CTNode("Person"))
    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasLabelSwedish,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row(0L, true, true, 23L, "Mats"),
      Row(1L, true, false, 42L, "Martin"),
      Row(2L, true, false, 1337L, "Max"),
      Row(3L, true, false, 9L, "Stefan")
    )
    verify(nodes, cols, data)
  }

  it("Specific node scan from mixed node CapsRecords") {
    val inputGraph = initGraph(`:Person` + `:Book`)
    val inputNodes = inputGraph.nodes("n")

    val singleTableGraph = caps.graphs.singleTableGraph(inputNodes, inputGraph.schema, Set(0))
    val nodes = singleTableGraph.nodes("n", CTNode("Person"))

    val cols = Seq(
      n,
      nHasLabelPerson,
      nHasLabelSwedish,
      nHasPropertyLuckyNumber,
      nHasPropertyName
    )
    val data = Bag(
      Row(0L, true, true, 23L, "Mats"),
      Row(1L, true, false, 42L, "Martin"),
      Row(2L, true, false, 1337L, "Max"),
      Row(3L, true, false, 9L, "Stefan")
    )
    verify(nodes, cols, data)
  }

  it("Node scan for missing label") {
    val inputGraph = initGraph(`:Book`)
    val inputNodes = inputGraph.nodes("n")

    val singleTableGraph = caps.graphs.singleTableGraph(inputNodes, inputGraph.schema, Set(0))

    singleTableGraph.nodes("n", CTNode("Person")).table.df.collect().toSet shouldBe empty
  }

  it("Supports .cypher node scans") {
    val singleTableGraph = initPersonReadsBookGraph

    singleTableGraph.cypher("MATCH (p:Person {name: 'Mats'}) RETURN p.luckyNumber").records.collect.toBag should equal(
      Bag(CypherMap("p.luckyNumber" -> 23)))
  }

  it("should create a single relationship between unique merged node pair") {
    val given = initGraph(
      """
        |CREATE (a: Person)
        |CREATE (b: Person)
        |CREATE (a)-[:HAS_INTEREST]->(i1:Interest {val: 1})
        |CREATE (a)-[:HAS_INTEREST]->(i2:Interest {val: 2})
        |CREATE (a)-[:HAS_INTEREST]->(i3:Interest {val: 3})
        |CREATE (a)-[:KNOWS]->(b)
      """.stripMargin)

    val when = given.cypher(
      """
        |MATCH (i:Interest)<-[h:HAS_INTEREST]-(a:Person)-[k:KNOWS]->(b:Person)
        |WITH DISTINCT a, b
        |CONSTRUCT
        |  CLONE a, b
        |  NEW (a)-[f:FOO]->(b)
        |RETURN GRAPH
      """.stripMargin)

    when.graph.relationships("f", CTRelationship("FOO")).size should equal(1)
  }

  it("implictly clones when creating a single relationship between unique merged node pair") {
    val given = initGraph(
      """
        |CREATE (a: Person)
        |CREATE (b: Person)
        |CREATE (a)-[:HAS_INTEREST]->(i1:Interest {val: 1})
        |CREATE (a)-[:HAS_INTEREST]->(i2:Interest {val: 2})
        |CREATE (a)-[:HAS_INTEREST]->(i3:Interest {val: 3})
        |CREATE (a)-[:KNOWS]->(b)
      """.stripMargin)

    val when = given.cypher(
      """
        |MATCH (i:Interest)<-[h:HAS_INTEREST]-(a:Person)-[k:KNOWS]->(b:Person)
        |WITH DISTINCT a, b
        |CONSTRUCT
        |  NEW (a)-[f:FOO]->(b)
        |RETURN GRAPH
      """.stripMargin)

    when.graph.relationships("f", CTRelationship("FOO")).size should equal(1)
  }

  ignore("should create a relationship for each copy of a node pair") {
    val given = initGraph(
      """
        |CREATE (a: Person)
        |CREATE (b: Person)
        |CREATE (a)-[:HAS_INTEREST]->(i1:Interest {val: 1})
        |CREATE (a)-[:HAS_INTEREST]->(i2:Interest {val: 2})
        |CREATE (a)-[:HAS_INTEREST]->(i3:Interest {val: 3})
        |CREATE (a)-[:KNOWS]->(b)
      """.stripMargin)

    val when = given.cypher(
      """
        |MATCH (i:Interest)<-[h:HAS_INTEREST]-(a:Person)-[k:KNOWS]->(b:Person)
        |CONSTRUCT
        |  CLONE a, b
        |  NEW (a)-[:FOO]->(b)
        |RETURN GRAPH
      """.stripMargin)

    when.graph.relationships("f", CTRelationship("FOO")).size should equal(3)
  }

  // TODO: Needs COPY OF to work again
  ignore("should merge and copy nodes") {
    val given = initGraph(
      """
        |CREATE (a: Person)
        |CREATE (b: Person)
        |CREATE (a)-[:HAS_INTEREST]->(i1:Interest {val: 1})
        |CREATE (a)-[:HAS_INTEREST]->(i2:Interest {val: 2})
        |CREATE (a)-[:HAS_INTEREST]->(i3:Interest {val: 3})
        |CREATE (a)-[:KNOWS]->(b)
      """.stripMargin)

    val when = given.cypher(
      """
        |MATCH (i:Interest)<-[h:HAS_INTEREST]-(a:Person)-[k:KNOWS]->(b:Person)
        |CONSTRUCT
        |  CLONE a
        |  NEW (a)
        |RETURN GRAPH
      """.stripMargin)

    when.graph.nodes("n", CTNode("Person")).size should equal(4)
  }

  private def initPersonReadsBookGraph: RelationalCypherGraph[DataFrameTable] = {
    val inputGraph = initGraph(`:Person` + `:Book` + `:READS`)

    val persons = inputGraph.nodes("p", CTNode("Person"))
    val reads = inputGraph.relationships("r", CTRelationship("READS"))
    val books = inputGraph.nodes("b", CTNode("Book"))

    val personReadsBookHeader = persons.header.join(reads.header).join(books.header)

    val personReadsBook = persons.table
      .join(reads.table, InnerJoin, persons.header.column(Var("p")(CTNode)) -> reads.header.column(rStart))
      .join(books.table, InnerJoin, reads.header.column(rEnd) -> books.header.column(Var("b")(CTNode)))

    val personReadsBookCapsRecords = caps.records.from(personReadsBookHeader, personReadsBook)

    caps.graphs.singleTableGraph(personReadsBookCapsRecords, inputGraph.schema, Set(0))
  }
}
