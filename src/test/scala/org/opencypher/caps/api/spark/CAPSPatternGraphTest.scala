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
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.record.CAPSRecordHeader
import org.opencypher.caps.impl.syntax.header.{addContents, _}
import org.opencypher.caps.ir.api.{Label, PropertyKey}
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag
import scala.collection.JavaConverters._

class CAPSPatternGraphTest extends CAPSTestSuite {
  import CAPSGraphTestData._

  test("project pattern graph") {
    val inputGraph = TestGraph(`:Person`).graph

    val person = inputGraph.cypher(
      """MATCH (a:Swedish)
        |RETURN GRAPH result OF (a)
      """.stripMargin)

    person.graphs("result").cypher("MATCH (n) RETURN n.name").recordsWithDetails.toLocalScalaIterator.toSet should equal(Set(
      CypherMap("n.name" -> CypherString("Mats"))
    ))
  }

  test("project pattern graph with relationship") {
    val inputGraph = TestGraph(`:Person` + `:KNOWS`).graph

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |RETURN GRAPH result OF (a)-[r]->(b)
      """.stripMargin)

    person.graphs("result").cypher("MATCH (n) RETURN n.name").recordsWithDetails.toLocalScalaIterator.toSet should equal(Set(
      CypherMap("n.name" -> CypherString("Mats")),
      CypherMap("n.name" -> CypherString("Stefan")),
      CypherMap("n.name" -> CypherString("Martin")),
      CypherMap("n.name" -> CypherString("Max"))
    ))
  }

  // TODO: Generate names for GRAPH OF pattern parts in frontend
  test("project pattern graph with created relationship") {
    val inputGraph = TestGraph(`:Person` + `:KNOWS`).graph

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |RETURN GRAPH result OF (a)-[foo:SWEDISH_KNOWS]->(b)
      """.stripMargin)

    person.graphs("result").cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n.name").
      recordsWithDetails.toLocalScalaIterator.toSet should equal(Set(
      CypherMap("n.name" -> CypherString("Stefan")),
      CypherMap("n.name" -> CypherString("Martin")),
      CypherMap("n.name" -> CypherString("Max"))
    ))
  }

  test("project pattern graph with created node") {
    val inputGraph = TestGraph(`:Person` + `:KNOWS`).graph

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |RETURN GRAPH result OF (a)-[foo:SWEDISH_KNOWS]->(bar)
      """.stripMargin)

    person.graphs("result").cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n").
      recordsWithDetails.toLocalScalaIterator.toSet should equal(Set(
      CypherMap("n" -> CypherNode(4294967296000L, NodeData.empty)),
      CypherMap("n" -> CypherNode(4294967296001L, NodeData.empty)),
      CypherMap("n" -> CypherNode(4294967296002L, NodeData.empty))
    ))
  }

  test("project pattern graph with created node with labels") {
    val inputGraph = TestGraph(`:Person` + `:KNOWS`).graph

    val person = inputGraph.cypher(
      """MATCH (a:Person:Swedish)-[r]->(b)
        |RETURN GRAPH result OF (a)-[foo:SWEDISH_KNOWS]->(bar:Foo)
      """.stripMargin)

    val graph = person.graphs("result")

    graph.cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n").
      recordsWithDetails.toCypherMaps.collect().map(_.toString).toSet should equal(Set(
      CypherMap("n" -> CypherNode(4294967296000L, NodeData(Seq("Foo"), Properties.empty))),
      CypherMap("n" -> CypherNode(4294967296001L, NodeData(Seq("Foo"), Properties.empty))),
      CypherMap("n" -> CypherNode(4294967296002L, NodeData(Seq("Foo"), Properties.empty)))
    ).map(_.toString))

    graph.nodes("n").toCypherMaps.collect().map(_.toString).toSet should equal(Set(
      CypherMap("n" -> CypherNode(0L, NodeData(Seq("Person", "Swedish"), Properties("name" -> "Mats", "luckyNumber" -> 23)))),
      CypherMap("n" -> CypherNode(4294967296000L, NodeData(Seq("Foo"), Properties.empty))),
      CypherMap("n" -> CypherNode(4294967296001L, NodeData(Seq("Foo"), Properties.empty))),
      CypherMap("n" -> CypherNode(4294967296002L, NodeData(Seq("Foo"), Properties.empty)))
    ).map(_.toString))
  }

  //TODO: Test creating literal property value
  //TODO: Test creating computed property value

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

  test("Returns only distinct results") {
    val p = 'p -> CTNode("Person")
    val fields = Seq(
      OpaqueField(p),
      ProjectedExpr(HasLabel(p, Label("Person"))(CTBoolean)),
      ProjectedExpr(Property(p, PropertyKey("name"))(CTString))
    )
    val (header, _) = RecordHeader.empty.update(addContents(fields))

    val sparkHeader = CAPSRecordHeader.asSparkStructType(header)
    val df = session.createDataFrame(List(
      Row(0L, true, "PersonPeter"),
      Row(0L, true, "PersonPeter")
    ).asJava, sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)

    val patternGraph = CAPSGraph.create(CAPSRecords.create(header, df), schema)

    patternGraph.nodes("n", CTNode).details.toMaps should equal(Bag(
      CypherMap("n" ->  0L, "n.name" -> "PersonPeter", "n:Person" -> true)
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
