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
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.fixture.RecordsVerificationFixture
import org.opencypher.spark.testing.support.creation.caps.{CAPSPatternGraphFactory, CAPSTestGraphFactory}

import scala.collection.JavaConverters._

class CAPSPatternGraphTest extends CAPSGraphTest with RecordsVerificationFixture {

  import CAPSGraphTestData._

  override def capsGraphFactory: CAPSTestGraphFactory = CAPSPatternGraphFactory

  it("projects a pattern graph") {
    val inputGraph = initGraph(`:Person`)

    val person = inputGraph.cypher(
      """MATCH (a :Swedish)
        |CONSTRUCT
        |  CLONE a
        |RETURN GRAPH
      """.stripMargin)

    person.getGraph.cypher("MATCH (n) RETURN n.name").getRecords.collect.toSet should equal(
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

    person.getGraph.cypher("MATCH (n) RETURN n.name").getRecords.collect.toSet should equal(
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
      .getGraph
      .cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n.name")
      .getRecords
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
      .getGraph
      .cypher("MATCH ()-[:SWEDISH_KNOWS]->(n) RETURN n.name")
      .getRecords
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
      .getGraph
      .cypher("MATCH (b)-[:KNOWS_A]->(n) WITH COUNT(n) as cnt RETURN cnt")
      .getRecords
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
      .getGraph
      .cypher("MATCH (b)-[:KNOWS_A]->(n) WITH COUNT(n) as cnt RETURN cnt")
      .getRecords
      .collect
      .toSet should equal(
      Set(
        CypherMap("cnt" -> 3)
      ))
  }

  it("relationship scan for specific type") {
    val inputGraph = initGraph(`:KNOWS` + `:READS`)
    val inputRels = inputGraph.relationships("r")

    val patternGraph = CAPSGraph.create(inputRels, inputGraph.schema.asCaps)
    val outputRels = patternGraph.relationships("r", CTRelationship("KNOWS"))

    outputRels.df.count() shouldBe 6
  }

  it("relationship scan for disjunction of types") {
    val inputGraph = initGraph(`:KNOWS` + `:READS` + `:INFLUENCES`)
    val inputRels = inputGraph.relationships("r")

    val patternGraph = CAPSGraph.create(inputRels, inputGraph.schema.asCaps)
    val outputRels = patternGraph.relationships("r", CTRelationship("KNOWS", "INFLUENCES"))

    outputRels.df.count() shouldBe 7
  }

  it("relationship scan for all types") {
    val inputGraph = initGraph(`:KNOWS` + `:READS`)
    val inputRels = inputGraph.relationships("r")

    val patternGraph = CAPSGraph.create(inputRels, inputGraph.schema)
    val outputRels = patternGraph.relationships("r", CTRelationship)

    outputRels.df.count() shouldBe 10
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

    val graph = person.getGraph

    graph.cypher("MATCH (n:Swede) RETURN labels(n)").getRecords.collect.toSet should equal(
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

    val graph = person.getGraph

    graph.cypher("MATCH (n:Swede) RETURN labels(n)").getRecords.collect.toSet should equal(
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

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.toDF().columns should equal(
      Array(
        "n",
        "____n:Person",
        "____n:Swedish",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_nameSTRING"
      ))

    outputNodes.toDF().collect().toSet should equal(
      Set(
        Row(0L, true, true, 23L, "Mats"),
        Row(1L, true, false, 42L, "Martin"),
        Row(2L, true, false, 1337L, "Max"),
        Row(3L, true, false, 9L, "Stefan")
      ))
  }

  it("Node scan from mixed node CapsRecords") {
    val inputGraph = initGraph(`:Person` + `:Book`)
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.toDF().columns.toSet should equal(
      Set(
        "n",
        "____n:Person",
        "____n:Swedish",
        "____n:Book",
        "____n_dot_nameSTRING",
        "____n_dot_luckyNumberINTEGER",
        "____n_dot_yearINTEGER",
        "____n_dot_titleSTRING"
      ))

    Bag(outputNodes.toDF().collect(): _*) should equal(
      Bag(
        Row(0L, false, true, true, 23L, "Mats", null, null),
        Row(1L, false, true, false, 42L, "Martin", null, null),
        Row(2L, false, true, false, 1337L, "Max", null, null),
        Row(3L, false, true, false, 9L, "Stefan", null, null),
        Row(4L, true, false, false, null, null, "1984", 1949L),
        Row(5L, true, false, false, null, null, "Cryptonomicon", 1999L),
        Row(6L, true, false, false, null, null, "The Eye of the World", 1990L),
        Row(7L, true, false, false, null, null, "The Circle", 2013L)
      ))
  }

  it("Node scan from multiple connected nodes") {
    val patternGraph = initPersonReadsBookGraph
    val nodes = patternGraph.nodes("n")
    val cols = Seq(
      "n",
      "____n:Book",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_nameSTRING",
      "____n_dot_titleSTRING",
      "____n_dot_yearINTEGER"
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
    val patternGraph = initPersonReadsBookGraph
    val nodes = patternGraph.nodes("n", CTNode("Person"))
    val cols = Seq(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_nameSTRING"
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

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)
    val nodes = patternGraph.nodes("n", CTNode("Person"))

    val cols = Seq(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_nameSTRING"
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

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema)

    patternGraph.nodes("n", CTNode("Person")).toDF().collect().toSet shouldBe empty
  }

  it("Supports .cypher node scans") {
    val patternGraph = initPersonReadsBookGraph

    patternGraph.cypher("MATCH (p:Person {name: 'Mats'}) RETURN p.luckyNumber").getRecords.collect.toBag should equal(
      Bag(CypherMap("p.luckyNumber" -> 23)))
  }

  it("Supports node scans from ad-hoc table") {
    val n: Var = Var("n")(CTNode)
    val exprs = Set(
      Var("p")(CTNode("Person")),
      n,
      HasLabel(n, Label("Person"))(CTBoolean),
      Var("q")(CTNode("Foo"))
    )
    val header = RecordHeader.from(exprs)

    val df = sparkSession.createDataFrame(
      List(
        Row(0L, 1L, true, 2L),
        Row(10L, 11L, false, 12L)
      ).asJava,
      header.toStructType)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")()
      .withNodePropertyKeys("Foo")()
      .asCaps

    val patternGraph = CAPSGraph.create(CAPSRecords(header, df), schema)

    patternGraph.nodes("n", CTNode("Person")).collect.toBag should equal(
      Bag(
        CypherMap("n" -> CAPSNode(0L, Set())),
        CypherMap("n" -> CAPSNode(1L, Set("Person"))),
        CypherMap("n" -> CAPSNode(10L, Set()))
      ))
  }

  ignore("Supports node scans from ad-hoc table 2") {
    val p: Var = Var("p")(CTNode)
    val c: Var = Var("c")(CTNode)
    val x: Var = Var("x")(CTRelationship("IN"))
    val exprs = Seq(
      c,
      HasLabel(c, Label("Customer"))(CTBoolean),
      Var("cName")(CTString.nullable),
      Property(c, PropertyKey("name"))(CTString.nullable),
      p,
      HasLabel(p, Label("Person"))(CTBoolean),
      Var("pName")(CTString.nullable), Property(p, PropertyKey("name"))(CTString),
      Property(p, PropertyKey("region"))(CTString),
      StartNode(x)(CTInteger),
      EndNode(x)(CTInteger),
      x,
      Type(x)(CTString)
    )
    val header = RecordHeader.from(exprs)

    val df = sparkSession.createDataFrame(
      List(
        Row(2001L, true, "Alice", 4L, true, "Alice", "US", 2001L, 4L, 4982162063360L, "IS"),
        Row(2002L, true, "Bob", 5L, true, "Bob", "US", 2002L, 5L, 4939212390400L, "IS"),
        Row(2008L, true, "Trudy", 11L, true, "Trudy", "EU", 2008L, 11L, 4947802324992L, "IS"),
        Row(2005L, true, "Carl", 8L, true, "Carl", "US", 2005L, 8L, 4698694221824L, "IS"),
        Row(2010L, true, "Oscar", 13L, true, "Oscar", "EU", 2010L, 13L, 5162550689792L, "IS"),
        Row(2011L, true, "Victor", 14L, true, "Victor", "EU", 2011L, 14L, 5076651343872L, "IS"),
        Row(2012L, true, "Peggy", 15L, true, "Peggy", "EU", 2012L, 15L, 5677946765312L, "IS"),
        Row(2006L, true, "Dave", 9L, true, "Dave", "US", 2006L, 9L, 5506148073472L, "IS"),
        Row(2009L, true, "Trent", 12L, true, "Trent", "EU", 2009L, 12L, 4466765987840L, "IS"),
        Row(2007L, true, "Mallory", 10L, true, "Mallory", "EU", 2007L, 10L, 5849745457152L, "IS"),
        Row(2003L, true, "Eve", 6L, true, "Eve", "US", 2003L, 6L, 5480378269696L, "IS"),
        Row(2004L, true, "Carol", 7L, true, "Carol", "US", 2004L, 7L, 5626407157760L, "IS")
      ).asJava,
      header.toStructType
    )

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString, "region" -> CTString)
      .withNodePropertyKeys("Customer")("name" -> CTString.nullable)
      .withRelationshipType("IN")
      .asCaps

    val patternGraph = CAPSGraph.create(CAPSRecords(header, df), schema)

    //    patternGraph.nodes("n").toCypherMaps.collect().toSet should equal(Set(
    //      CypherMap("n" ->  0L),
    //      CypherMap("n" ->  1L),
    //      CypherMap("n" -> 10L)
    //    ))
  }

  it("Returns only distinct results") {
    val p = Var("p")(CTNode("Person"))
    val exprs = Seq(
      p,
      HasLabel(p, Label("Person"))(CTBoolean),
      Property(p, PropertyKey("name"))(CTString)
    )
    val header = RecordHeader.from(exprs)

    val sparkHeader = header.toStructType
    val df = sparkSession.createDataFrame(
      List(
        Row(0L, true, "PersonPeter"),
        Row(0L, true, "PersonPeter")
      ).asJava,
      sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .asCaps

    val patternGraph = CAPSGraph.create(CAPSRecords(header, df), schema)

    patternGraph.nodes("n", CTNode).collect.toBag should equal(
      Bag(
        CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "PersonPeter"))))
    )
  }

  it("Supports node scans when different variables have the same property keys") {
    val p = Var("p")(CTNode("Person"))
    val e = Var("e")(CTNode("Employee"))
    val exprs = Seq(
      p,
      HasLabel(p, Label("Person"))(CTBoolean),
      e,
      HasLabel(e, Label("Employee"))(CTBoolean),
      Property(p, PropertyKey("name"))(CTString),
      Var("foo")(CTString), Property(e, PropertyKey("name"))(CTString)
    )
    val header = RecordHeader.from(exprs)

    val sparkHeader = header.toStructType
    val df = sparkSession.createDataFrame(
      List(
        Row(0L, true, 1L, true, "PersonPeter", "EmployeePeter"),
        Row(10L, true, 11L, true, "PersonSusanna", "EmployeeSusanna")
      ).asJava,
      sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString)
      .asCaps

    val patternGraph = CAPSGraph.create(CAPSRecords(header, df), schema)

    patternGraph.nodes("n", CTNode).collect.toBag should equal(
      Bag(
        CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "PersonPeter"))),
        CypherMap("n" -> CAPSNode(1L, Set("Employee"), CypherMap("name" -> "EmployeePeter"))),
        CypherMap("n" -> CAPSNode(10L, Set("Person"), CypherMap("name" -> "PersonSusanna"))),
        CypherMap("n" -> CAPSNode(11L, Set("Employee"), CypherMap("name" -> "EmployeeSusanna")))
      )
    )
  }

  //TODO: Requires changes to schema verification
  ignore("Supports node scans when variables have the same label and property") {
    val p = Var("p")(CTNode("Person"))
    val e = Var("e")(CTNode("Employee"))
    val pe = Var("pe")(CTNode("Person", "Employee"))
    val exprs = Seq(
      p,
      e,
      pe,
      Property(p, PropertyKey("name"))(CTString),
      Property(e, PropertyKey("name"))(CTString.nullable),
      Property(pe, PropertyKey("name"))(CTString)
    )
    val header = RecordHeader.from(exprs)

    val sparkHeader = header.toStructType
    val df = sparkSession.createDataFrame(
      List(
        Row(0L, 1L, 2L, "PersonPeter", "EmployeePeter", "HybridPeter"),
        Row(10L, 11L, 12L, "person.graphsusanna", null, "HybridSusanna")
      ).asJava,
      sparkHeader)

    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString.nullable)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString)
      .asCaps

    val patternGraph = CAPSGraph.create(CAPSRecords(header, df), schema)

    patternGraph.nodes("n", CTNode).toMaps should equal(
      Bag(
        CypherMap("n" -> 0L, "n.name" -> "PersonPeter", "n:Person" -> true, "n:Employee" -> false),
        CypherMap("n" -> 1L, "n.name" -> "EmployeePeter", "n:Person" -> false, "n:Employee" -> true),
        CypherMap("n" -> 2L, "n.name" -> "HybridPeter", "n:Person" -> true, "n:Employee" -> true),
        CypherMap("n" -> 10L, "n.name" -> "person.graphsusanna", "n:Person" -> true, "n:Employee" -> false),
        CypherMap("n" -> 11L, "n.name" -> null, "n:Person" -> false, "n:Employee" -> true),
        CypherMap("n" -> 12L, "n.name" -> "HybridSusanna", "n:Person" -> true, "n:Employee" -> true)
      ))
  }

  // TODO: Rewrite with equivalence merge
  ignore("Reduce cardinality of the pattern graph base table") {
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
        |  CLONE a, k, b
        |  NEW (a)-[k]->(b)
        |RETURN GRAPH
      """.stripMargin)

    when.getGraph.asInstanceOf[CAPSPatternGraph].baseTable.df.count() should equal(3)
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

    when.getGraph.relationships("f", CTRelationship("FOO")).size should equal(1)
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

    when.getGraph.relationships("f", CTRelationship("FOO")).size should equal(1)
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

    when.getGraph.relationships("f", CTRelationship("FOO")).size should equal(3)
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

    when.getGraph.nodes("n", CTNode("Person")).size should equal(4)
  }

  private def initPersonReadsBookGraph: CAPSGraph = {
    val inputGraph = initGraph(`:Person` + `:Book` + `:READS`)

    val books = inputGraph.nodes("b", CTNode("Book"))
    val booksDf = books.toDF().as("b")
    val reads = inputGraph.relationships("r", CTRelationship("READS"))
    val readsDf = reads.toDF().as("r")
    val persons = inputGraph.nodes("p", CTNode("Person"))
    val personsDf = persons.toDF().as("p")

    val joinedDf = personsDf
      .join(readsDf, personsDf.col("p") === readsDf.col("____source(r)"))
      .join(booksDf, readsDf.col("____target(r)") === booksDf.col("b"))

    val exprs = persons.header.expressions ++ reads.header.expressions ++ books.header.expressions
    val joinHeader = RecordHeader.from(exprs)

    CAPSGraph.create(CAPSRecords(joinHeader, joinedDf), inputGraph.schema)
  }
}
