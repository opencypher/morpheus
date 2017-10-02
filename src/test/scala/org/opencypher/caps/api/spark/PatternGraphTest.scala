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
package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr.{HasLabel, Property, Var}
import org.opencypher.caps.api.record.{OpaqueField, ProjectedExpr, ProjectedField, RecordHeader}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTRelationship, CTString}
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.impl.record.CAPSRecordHeader
import org.opencypher.caps.impl.syntax.header.{addContents, _}
import org.opencypher.caps.ir.api.global.{Label, PropertyKey}
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag
import scala.collection.JavaConverters._

class PatternGraphTest extends CAPSTestSuite {
  import CAPSGraphTestData._

  test("Node scan from single node CAPSRecords") {
    val inputGraph = TestGraph(`:Person`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal (Set(
      Row(0L, true, true,    "Mats",   23L),
      Row(1L, true, false, "Martin",   42L),
      Row(2L, true, false,    "Max", 1337L),
      Row(3L, true, false, "Stefan",    9L)
    ))
  }

  test("Node scan from mixed node CapsRecords") {
    val inputGraph = TestGraph(`:Person` + `:Book`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_yearINTEGER",
      "____n_dot_titleSTRING"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0L,  true,  true,  false,   "Mats",   23L, null,                   null),
      Row(1L,  true,  false, false, "Martin",   42L, null,                   null),
      Row(2L,  true,  false, false,    "Max", 1337L, null,                   null),
      Row(3L,  true,  false, false, "Stefan",    9L, null,                   null),
      Row(4L, false,  false,  true,     null, null, 1949L,                 "1984"),
      Row(5L, false,  false,  true,     null, null, 1999L,        "Cryptonomicon"),
      Row(6L, false,  false,  true,     null, null, 1990L, "The Eye of the World"),
      Row(7L, false,  false,  true,     null, null, 2013L,           "The Circle")
    ))
  }

  test("Node scan from multiple connected nodes") {
    val patternGraph = initPersonReadsBookGraph
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_yearINTEGER",
      "____n_dot_titleSTRING"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0L,  true,  true,  false,   "Mats",   23L, null,                   null),
      Row(1L,  true,  false, false, "Martin",   42L, null,                   null),
      Row(2L,  true,  false, false,    "Max", 1337L, null,                   null),
      Row(3L,  true,  false, false, "Stefan",    9L, null,                   null),
      Row(4L, false,  false,  true,     null, null, 1949L,                 "1984"),
      Row(5L, false,  false,  true,     null, null, 1999L,        "Cryptonomicon"),
      Row(6L, false,  false,  true,     null, null, 1990L, "The Eye of the World"),
      Row(7L, false,  false,  true,     null, null, 2013L,           "The Circle")
    ))
  }

  test("Specific node scan from multiple connected nodes") {
    val patternGraph = initPersonReadsBookGraph

    val outputNodes = patternGraph.nodes("n", CTNode("Person"))

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0L,  true,   true,   "Mats",   23L),
      Row(1L,  true,  false, "Martin",   42L),
      Row(2L,  true,  false,    "Max", 1337L),
      Row(3L,  true,  false, "Stefan",    9L)
    ))
  }

  test("Specific node scan from mixed node CapsRecords") {
    val inputGraph = TestGraph(`:Person` + `:Book`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val outputNodes = patternGraph.nodes("n", CTNode("Person"))

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0L,  true,  true,   "Mats",    23L),
      Row(1L,  true,  false, "Martin",   42L),
      Row(2L,  true,  false,    "Max", 1337L),
      Row(3L,  true,  false, "Stefan",    9L)
    ))
  }

  // TODO: Deal with non-existing tokens gracefully
  ignore("Node scan for missing label") {
    val inputGraph = TestGraph(`:Book`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)

    patternGraph.nodes("n", CTNode("Person")).details.toDF().collect().toSet shouldBe empty
  }

  test("Supports .cypher node scans") {
    val patternGraph = initPersonReadsBookGraph

    patternGraph.cypher("MATCH (p:Person {name: 'Mats'}) RETURN p.luckyNumber").records.toMaps should equal(Bag(CypherMap("p.luckyNumber" -> 23)))
  }

  test("Supports node scans from ad-hoc table") {
    val n: Var = 'n -> CTNode
    val fields = Seq(
      OpaqueField('p -> CTNode("Person")),
      OpaqueField(n),
      ProjectedExpr(HasLabel(n, Label("Person"))(CTBoolean)),
      OpaqueField('q -> CTNode("Foo"))
    )
    val (header, _) = RecordHeader.empty.update(addContents(fields))

    val df = session.createDataFrame(List(
      Row( 0L,  1L, true,   2L),
      Row(10L, 11L, false, 12L)
    ).asJava, CAPSRecordHeader.asSparkStructType(header))

    val schema = Schema.empty
      .withNodePropertyKeys("Person")()
      .withNodePropertyKeys("Foo")()

    val patternGraph = CAPSGraph.create(CAPSRecords.create(header, df), schema)

    patternGraph.nodes("n", CTNode("Person")).toMaps should equal(Bag(
      CypherMap("n" ->  0L),
      CypherMap("n" ->  1L),
      CypherMap("n" -> 10L)
    ))
  }

  test("Supports node scans when different variables have the same property keys") {
    val p = 'p -> CTNode("Person")
    val e = 'e -> CTNode("Employee")
    val fields = Seq(
      OpaqueField(p),
      ProjectedExpr(HasLabel(p, Label("Person"))(CTBoolean)),
      OpaqueField(e),
      ProjectedExpr(HasLabel(e, Label("Employee"))(CTBoolean)),
      ProjectedExpr(Property(p, PropertyKey("name"))(CTString)),
      ProjectedField('foo -> CTString, Property(e, PropertyKey("name"))(CTString))
    )
    val (header, _) = RecordHeader.empty.update(addContents(fields))

    val sparkHeader = CAPSRecordHeader.asSparkStructType(header)
    val df = session.createDataFrame(List(
      Row( 0L, true, 1L, true, "PersonPeter", "EmployeePeter"),
      Row(10L, true, 11L, true, "PersonSusanna", "EmployeeSusanna")
    ).asJava, sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString)

    val patternGraph = CAPSGraph.create(CAPSRecords.create(header, df), schema)

    patternGraph.nodes("n", CTNode).details.toMaps should equal(Bag(
      CypherMap("n" ->  0L, "n.name" -> "PersonPeter", "n:Person" -> true, "n:Employee" -> false),
      CypherMap("n" ->  1L, "n.name" -> "EmployeePeter", "n:Person" -> false, "n:Employee" -> true),
      CypherMap("n" -> 10L, "n.name" -> "PersonSusanna", "n:Person" -> true, "n:Employee" -> false),
      CypherMap("n" -> 11L, "n.name" -> "EmployeeSusanna", "n:Person" -> false, "n:Employee" -> true)
    ))
  }

  //TODO: Requires changes to schema verification
  ignore("Supports node scans when variables have the same label and property") {
    val p = 'p -> CTNode("Person")
    val e = 'e -> CTNode("Employee")
    val pe = 'pe -> CTNode("Person", "Employee")
    val fields = Seq(
      OpaqueField(p),
      OpaqueField(e),
      OpaqueField(pe),
      ProjectedExpr(Property(p, PropertyKey("name"))(CTString)),
      ProjectedExpr(Property(e, PropertyKey("name"))(CTString.nullable)),
      ProjectedExpr(Property(pe, PropertyKey("name"))(CTString))
    )
    val (header, _) = RecordHeader.empty.update(addContents(fields))

    val sparkHeader = CAPSRecordHeader.asSparkStructType(header)
    val df = session.createDataFrame(List(
      Row(0L, 1L, 2L, "PersonPeter", "EmployeePeter", "HybridPeter"),
      Row(10L, 11L, 12L, "PersonSusanna", null, "HybridSusanna")
    ).asJava, sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString.nullable)
      .withLabelCombination("Person" -> "Employee")

    val patternGraph = CAPSGraph.create(CAPSRecords.create(header, df), schema)

    patternGraph.nodes("n", CTNode).details.toMaps should equal(Bag(
      CypherMap("n" -> 0L, "n.name" -> "PersonPeter", "n:Person" -> true, "n:Employee" -> false),
      CypherMap("n" -> 1L, "n.name" -> "EmployeePeter", "n:Person" -> false, "n:Employee" -> true),
      CypherMap("n" -> 2L, "n.name" -> "HybridPeter", "n:Person" -> true, "n:Employee" -> true),
      CypherMap("n" -> 10L, "n.name" -> "PersonSusanna", "n:Person" -> true, "n:Employee" -> false),
      CypherMap("n" -> 11L, "n.name" -> null, "n:Person" -> false, "n:Employee" -> true),
      CypherMap("n" -> 12L, "n.name" -> "HybridSusanna", "n:Person" -> true, "n:Employee" -> true)
    ))
  }

  //TODO: Distinct

//  test("Supports .cypher") {
//    val patternGraph = initPersonReadsBookGraph
//
//    patternGraph.cypher("""MATCH (p:Person {name: 'Mats'})-[:READS {recommends: true}]->(b:Book)
//                          |RETURN b.title""".stripMargin).records.toMaps should equal(Bag(
//      CypherMap("b.title" -> "1984")
//    ))
//  }

//
//  test("Construct graph from single node and single relationship scan") {
//    val graph = CAPSGraph.create(`:Person`, `:KNOWS`)
//    val rels  = graph.relationships("e")
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016)
//    ))
//  }
//
//  test("Extract all node scans") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`)
//
//    val nodes = graph.nodes("n", CTNode())
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Book",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER",
//      "____n_dot_titleSTRING",
//      "____n_dot_yearINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal(Set(
//      Row( 1,  true,  true,  false,   "Mats",   23,                   null, null),
//      Row( 2,  true,  false, false, "Martin",   42,                   null, null),
//      Row( 3,  true,  false, false,    "Max", 1337,                   null, null),
//      Row( 4,  true,  false, false, "Stefan",    9,                   null, null),
//      Row(10, false,  false,  true,     null, null,                 "1984", 1949),
//      Row(20, false,  false,  true,     null, null,        "Cryptonomicon", 1999),
//      Row(30, false,  false,  true,     null, null, "The Eye of the World", 1990),
//      Row(40, false,  false,  true,     null, null,           "The Circle", 2013)
//    ))
//  }
//
//  test("Extract node scan subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal (Set(
//      Row(1, true, true,    "Mats",   23),
//      Row(2, true, false, "Martin",   42),
//      Row(3, true, false,    "Max", 1337),
//      Row(4, true, false, "Stefan",    9)
//    ))
//  }
//
//  test("Extract all relationship scans") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)
//
//    val rels  = graph.relationships("e")
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER",
//      "____e_dot_recommendsBOOLEAN"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      // :KNOWS
//      Row(1, 1, 0, 2, 2017, null),
//      Row(1, 2, 0, 3, 2016, null),
//      Row(1, 3, 0, 4, 2015, null),
//      Row(2, 4, 0, 3, 2016, null),
//      Row(2, 5, 0, 4, 2013, null),
//      Row(3, 6, 0, 4, 2016, null),
//      // :READS
//      Row(1, 100, 1, 10, null, true),
//      Row(2, 200, 1, 40, null, true),
//      Row(3, 300, 1, 30, null, true),
//      Row(4, 400, 1, 20, null, false)
//    ))
//  }
//
//  test("Extract relationship scan subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)
//
//    val rels  = graph.relationships("e", CTRelationship("KNOWS"))
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016)
//    ))
//  }
//
//  test("Extract relationship scan strict subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`, `:INFLUENCES`)
//
//    val rels  = graph.relationships("e", CTRelationship("KNOWS", "INFLUENCES"))
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      // :KNOWS
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016),
//      // :INFLUENCES
//      Row(10, 1000, 2, 20, null)
//    ))
//  }
//
//  test("Extract from scans with overlapping labels") {
//    val graph = CAPSGraph.create(`:Person`, `:Programmer`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Programmer",
//      "____n_dot_languageSTRING",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal (Set(
//      Row(1,   true, true,  false, null,   "Mats",   23),
//      Row(2,   true, false, false, null, "Martin",   42),
//      Row(3,   true, false, false, null,    "Max", 1337),
//      Row(4,   true, false, false, null, "Stefan",    9),
//      Row(100, true, false, true,   "C",  "Alice",   42),
//      Row(200, true, false, true,   "D",    "Bob",   23),
//      Row(300, true, false, true,   "F",    "Eve",   84),
//      Row(400, true, false, true,   "R",   "Carl",   49)
//    ))
//  }
//
//  test("Extract from scans with implied label but missing keys") {
//    val graph = CAPSGraph.create(`:Person`, `:Brogrammer`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Brogrammer",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER",
//      "____n_dot_languageSTRING"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal(Set(
//      Row(1, true, true, false, "Mats", 23, null),
//      Row(2, true, false, false, "Martin", 42, null),
//      Row(3, true, false, false, "Max", 1337, null),
//      Row(4, true, false, false, "Stefan", 9, null),
//      Row(100, true, false, true, null, null, "Node"),
//      Row(200, true, false, true, null, null, "Coffeescript"),
//      Row(300, true, false, true, null, null, "Javascript"),
//      Row(400, true, false, true, null, null, "Typescript")
//    ))
//  }

  private def initPersonReadsBookGraph: CAPSGraph = {
    val inputGraph = TestGraph(`:Person` + `:Book` + `:READS`).graph

    val books = inputGraph.nodes("b", CTNode("Book"))
    val booksDf = books.details.toDF().as("b")
    val reads = inputGraph.relationships("r", CTRelationship("READS"))
    val readsDf = reads.details.toDF().as("r")
    val persons = inputGraph.nodes("p", CTNode("Person"))
    val personsDf = persons.details.toDF().as("p")

    val joinedDf = personsDf
      .join(readsDf, personsDf.col("p") === readsDf.col("____source(r)"))
      .join(booksDf, readsDf.col("____target(r)") === booksDf.col("b"))

    val slots = persons.details.header.slots ++ reads.details.header.slots ++ books.details.header.slots
    val joinHeader = RecordHeader.from(slots.map(_.content): _*)

    CAPSGraph.create(CAPSRecords.create(joinHeader, joinedDf), inputGraph.schema)
  }
}
