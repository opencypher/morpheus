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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.{CAPSPatternGraph, CAPSUnionGraph}
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class MultipleGraphBehaviour extends CAPSTestSuite with ScanGraphInit {

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  def testGraph3 = initGraph("CREATE (:Car {type: 'Toyota'})")

  def testGraphRels = initGraph(
    """|CREATE (mats:Person {name: 'Mats'})
       |CREATE (max:Person {name: 'Max'})
       |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
    """.stripMargin)

  it("CLONEs without an alias") {
    val query =
      """
        |MATCH (n)
        |CONSTRUCT
        |  CLONE n
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty
  }

  it("CLONEs with an alias") {
    val query =
      """
        |MATCH (n)
        |CONSTRUCT
        |  CLONE n as m
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty
  }

  it("should return a graph") {
    val query =
      """RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty

    result.asCaps.getGraph shouldMatch testGraph1
  }

  it("should switch to another graph and then return it") {
    caps.store("graph2", testGraph2)
    val query =
      """FROM GRAPH graph2
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty
    result.asCaps.getGraph shouldMatch testGraph2
  }

  it("can select a source graph to match data from") {
    caps.store("graph1", testGraph1)
    caps.store("graph2", testGraph2)

    val query =
      """FROM GRAPH graph2
        |MATCH (n:Person)
        |RETURN n.name AS name""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil")
      ))
  }

  it("matches from different graphs") {
    caps.store("graph1", testGraph1)
    caps.store("graph2", testGraph2)
    caps.store("graph3", testGraph3)

    val query =
      """FROM GRAPH graph2
        |MATCH (n:Person)
        |WITH n.name AS name
        |FROM GRAPH graph3
        |MATCH (c:Car)
        |RETURN name, c.type AS car""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil", "car" -> "Toyota")
      ))
  }

  it("should construct a graph") {
    val query =
      """|CONSTRUCT
         |  NEW (:A)-[:KNOWS]->(:B)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").size should equal(2)
    result.getGraph.relationships("r").size should equal(1)
  }

  it("should CONSTRUCT a graph with multiple connected NEW clauses") {
    val query =
      """|CONSTRUCT
         |  NEW (a:A)-[:KNOWS]->(b:B)
         |  NEW (b)-[:KNOWS]->(c:C)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A", "B", "C"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").size should equal(3)
    result.getGraph.relationships("r").size should equal(2)
  }

  it("should CONSTRUCT a graph with multiple unconnected NEW clauses") {
    val query =
      """|CONSTRUCT
         |  NEW (a:A)-[:KNOWS]->(b:B)
         |  NEW (c:C)-[:KNOWS]->(d:D)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A", "B", "C", "D"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").size should equal(4)
    result.getGraph.relationships("r").size should equal(2)
  }

  it("should CONSTRUCT a graph with multiple unconnected anonymous NEW clauses") {
    val query =
      """|CONSTRUCT
         |  NEW (:A)
         |  NEW (:B)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema.relationshipTypes should equal(Set.empty)
    result.getGraph.nodes("n").size should equal(2)
    result.getGraph.relationships("r").size should equal(0)
  }

  it("should construct a node property from a matched node") {
    val query =
      """|MATCH (m)
         |CONSTRUCT
         |  NEW (a :A { name: m.name})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A"))
    result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString).asCaps)
    result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }

  it("should construct a node property from a literal") {
    val query =
      """|CONSTRUCT
         |  NEW ({name: 'Donald'})
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys()("name" -> CTString).asCaps)
    result.getGraph.cypher("MATCH (a) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should construct a node label and a node property from a literal") {
    val query =
      """|CONSTRUCT
         |  NEW (a :A {name: 'Donald'})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A"))
    result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString).asCaps)
    result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should construct multiple properties") {
    val query =
      """|CONSTRUCT
         |  NEW (a:A:B {name:'Donald', age:100})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema should equal(
      Schema.empty
        .withNodePropertyKeys(Set("A", "B"), PropertyKeys("name" -> CTString, "age" -> CTInteger))
        .asCaps)
    result.getGraph.cypher("MATCH (a:A:B) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Donald")
    ))
  }

  it("should pick up labels of the outer match") {
    val query =
      """|MATCH (m:Person)
         |CONSTRUCT
         |  CLONE m
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("Person"))
    result.getGraph.schema should equal(
      Schema.empty
        .withNodePropertyKeys(Set("Person"), PropertyKeys("name" -> CTString))
        .asCaps)
    result.getGraph.cypher("MATCH (a:Person) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }

  it("should construct a relationship") {
    val query =
      """|CONSTRUCT
         |  NEW ()-[r:FOO {val : 42}]->()
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.relationshipTypes should equal(Set("FOO"))
    result.getGraph.schema should equal(Schema.empty
      .withNodePropertyKeys()()
      .withRelationshipPropertyKeys("FOO", PropertyKeys("val" -> CTInteger)).asCaps)
    result.getGraph.cypher("MATCH ()-[r]->() RETURN r.val").getRecords.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 42)
    ))
  }

  it("should copy a relationship") {
    val query =
      """|CONSTRUCT
         |  NEW ()-[r:FOO {val : 42}]->()
         |MATCH ()-[s]->()
         |CONSTRUCT
         |  NEW ()-[t COPY OF s {name : 'Donald'}]->()
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.cypher("MATCH ()-[r]->() RETURN r.val, r.name, type(r) as type").getRecords.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 42, "r.name" -> "Donald", "type" -> "FOO")
    ))
  }

  it("should copy a mean relationship") {
    val graph = initGraph(
      """
        |CREATE ()-[:FOO {val: 1, val2: 2}]->()
        |CREATE ()-[:BAR {val: 1, val2: 3}]->()
      """.stripMargin)

    val query =
      """MATCH ()-[s]->()
         |CONSTRUCT
         |  NEW ()-[t COPY OF s :BAZ {val2 : 'Donald'}]->()
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.asInstanceOf[CAPSUnionGraph].graphs.keys.tail.head.asInstanceOf[CAPSPatternGraph].baseTable.data.show()

    result.getGraph.cypher("MATCH ()-[r]->() RETURN r.val, r.val2, type(r) as type").getRecords.iterator.toBag should equal(Bag(
      CypherMap("r.val" -> 1, "r.val2" -> "Donald", "type" -> "BAZ"),
      CypherMap("r.val" -> 1, "r.val2" -> "Donald", "type" -> "BAZ")
    ))
  }

  it("should copy a node") {
    val query =
      """|CONSTRUCT
         |  NEW (:Foo {foo: 'bar'})
         |MATCH (a)
         |CONSTRUCT
         |  NEW (COPY OF a)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("Foo"))
    result.getGraph.schema should equal(Schema.empty
      .withNodePropertyKeys("Foo")("foo" -> CTString)
      .asCaps)

    result.getGraph.cypher("MATCH (a) RETURN a.foo, labels(a) as labels").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.foo" -> "bar", "labels" -> Seq("Foo"))
    ))
  }

  it("should copy a node with labels") {
    val graph = initGraph(
      """
        |CREATE (:A {val: 1})
        |CREATE (:B {val: 1})
        |CREATE (:A:C {val: 1})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  NEW (COPY OF a)
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.cypher("MATCH (a) RETURN a.val, labels(a) as labels").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> 1, "labels" -> Seq("A")),
      CypherMap("a.val" -> 1, "labels" -> Seq("B")),
      CypherMap("a.val" -> 1, "labels" -> Seq("A", "C"))
    ))
  }

  it("can override in SET") {
    val graph = initGraph(
      """
        |CREATE ({val: 1})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  NEW (COPY OF a {val: 2})
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.cypher("MATCH (a) RETURN a.val").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> 2)
    ))
  }

  it("can override heterogeneous types in SET") {
    val graph = initGraph(
      """
        |CREATE ({val: 1})
      """.stripMargin)

    val query =
      """|MATCH (a)
         |CONSTRUCT
         |  NEW (COPY OF a {val: 'foo'})
         |RETURN GRAPH""".stripMargin

    val result = graph.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.cypher("MATCH (a) RETURN a.val").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.val" -> "foo")
    ))
  }

  it("supports CLONE in CONSTRUCT") {
    val res = testGraph1.unionAll(testGraph2).cypher(
      """
        |MATCH (n),(m)
        |WHERE n.name = 'Mats' AND m.name = 'Phil'
        |CONSTRUCT
        | CLONE n, m
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 1
  }

  it("implicitly CLONEs in CONSTRUCT") {
    val res = testGraph1.unionAll(testGraph2).cypher(
      """
        |MATCH (n),(m)
        |WHERE n.name = 'Mats' AND m.name = 'Phil'
        |CONSTRUCT
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 1
  }

  it("constructs multiple relationships") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |WITH DISTINCT n, m
        |CONSTRUCT
        | CLONE n, m
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 2
  }

  it("implicitly clones when constructing multiple relationships") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |WITH DISTINCT n, m
        |CONSTRUCT
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 2
  }

  it("constructs multiple relationships 2") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |CONSTRUCT
        | CLONE n, m
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 3
  }

  it("implicitly clones when constructing multiple relationships 2") {
    val inputGraph = initGraph(
      """
        |CREATE (p0 {name: 'Mats'})
        |CREATE (p1 {name: 'Phil'})
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p0)-[:KNOWS]->(p1)
        |CREATE (p1)-[:KNOWS]->(p0)
      """.stripMargin)

    val res = inputGraph.cypher(
      """
        |MATCH (n)-[:KNOWS]->(m)
        |CONSTRUCT
        | NEW (n)-[r:KNOWS]->(m)
        |RETURN GRAPH
      """.stripMargin)

    res.getGraph.nodes("n").collect.length shouldBe 2
    res.getGraph.relationships("r").collect.length shouldBe 3
  }

  it("CONSTRUCTS ON a single graph") {
    caps.store("one", testGraph1)
    val query =
      """
        |CONSTRUCT ON one
        |RETURN GRAPH""".stripMargin

    val result = testGraph2.cypher(query).getGraph

    result.schema should equal(testGraph1.schema)
    result.nodes("n").toMaps should equal(testGraph1.nodes("n").toMaps)
    result.relationships("r").toMaps should equal(testGraph1.relationships("r").toMaps)
  }

  it("CONSTRUCTS ON two graphs") {
    caps.store("one", testGraph1)
    caps.store("two", testGraph2)
    val query =
      """
        |CONSTRUCT ON one, two
        |RETURN GRAPH""".stripMargin

    val result = testGraph2.cypher(query).getGraph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).asCaps)
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    result.relationships("r").toMaps should equal(testGraph1.unionAll(testGraph2).relationships("r").toMaps)
  }

  it("CONSTRUCTS ON two graphs and adds a relationship") {
    caps.store("one", testGraph1)
    caps.store("two", testGraph2)
    val query =
      """|FROM GRAPH one
         |MATCH (m: Person)
         |FROM GRAPH two
         |MATCH (p: Person)
         |CONSTRUCT ON one, two
         |  CLONE m, p
         |  NEW (m)-[:KNOWS]->(p)
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("KNOWS")().asCaps)
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    result.relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(2251799813685248L, 0L, 1125899906842624L, "KNOWS")))
    )
  }

  it("implictly clones when CONSTRUCTing ON two graphs and adding a relationship") {
    caps.store("one", testGraph1)
    caps.store("two", testGraph2)
    val query =
      """|FROM GRAPH one
         |MATCH (m: Person)
         |FROM GRAPH two
         |MATCH (p: Person)
         |CONSTRUCT ON one, two
         |  NEW (m)-[:KNOWS]->(p)
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("KNOWS")().asCaps)
    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
    result.relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(2251799813685248L, 0L, 1125899906842624L, "KNOWS")))
    )
  }

  it("constructs a new node") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a)
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(Schema.empty.withNodePropertyKeys(Set.empty[String]).asCaps)
    graph.asCaps.tags should equal(Set(0))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0))
    ))
  }

  it("construct match construct") {
    caps.store("g1", testGraphRels)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a)
        |CONSTRUCT // generated qgn
        |  CLONE a
        |MATCH (b)
        |CONSTRUCT
        |  ON g1
        |  CLONE b
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(testGraphRels.schema)
    graph.asCaps.tags should equal(Set(0, 1))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1, Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L.setTag(1), Set("Person"), CypherMap("name" -> "Max")))
    ))
  }

  it("does not clone twice when a variable is both constructed on and matched") {
    caps.store("g1", testGraph1)
    caps.store("g2", testGraph2)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a:Person)
        |FROM GRAPH g2
        |MATCH (b:Person)
        |CONSTRUCT
        |  ON g2
        |  CLONE a, b
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(testGraph1.schema.asCaps)
    graph.asCaps.tags should equal(Set(0, 1))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "Phil")))
    ))
  }

  it("allows CONSTRUCT ON with relationships") {
    caps.store("testGraphRels1", testGraphRels)
    caps.store("testGraphRels2", testGraphRels)
    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |CONSTRUCT ON testGraphRels2
         |  CLONE p1, r1, p2
         |  NEW (p1)-[ r1]->( p2)
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph

    result.schema should equal((testGraph1.schema ++ testGraph2.schema).withRelationshipPropertyKeys("HAS_SIMILAR_NAME")().asCaps)

    result.nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L, Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L.setTag(1), Set("Person"), CypherMap("name" -> "Max")))
    ))

    result.relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(2, 1, 0, "HAS_SIMILAR_NAME")),
      CypherMap("r" -> CAPSRelationship(2.setTag(1), 1.setTag(1), 0.setTag(1), "HAS_SIMILAR_NAME"))
    ))

    result.asCaps.tags should equal(Set(0, 1))
  }

  it("allows cloning from different graphs with nodes and relationships") {
    def testGraphRels = initGraph(
      """|CREATE (mats:Person {name: 'Mats'})
         |CREATE (max:Person {name: 'Max'})
         |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
      """.stripMargin)

    caps.store("testGraphRels1", testGraphRels)
    caps.store("testGraphRels2", testGraphRels)

    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |FROM GRAPH testGraphRels2
         |MATCH (p3 :Person)-[r2]->(p4 :Person)
         |CONSTRUCT
         |  CLONE p1, p2, p3, p4, r1, r2
         |  NEW (p1)-[r1]->(p2)
         |  NEW (p3)-[r2]->(p4)
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph
    result.schema.asCaps shouldEqual testGraphRels.schema

    result.nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L, Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L.setTag(1), Set("Person"), CypherMap("name" -> "Max")))
    ))

    result.relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(2, 1, 0, "HAS_SIMILAR_NAME")),
      CypherMap("r" -> CAPSRelationship(2.setTag(1), 1.setTag(1), 0.setTag(1), "HAS_SIMILAR_NAME"))
    ))
  }

  it("allows consecutive construction") {
    val query =
      """|CONSTRUCT
         |  NEW (a:A)-[r:FOO]->(b:B)
         |MATCH (a)-->(b)
         |CONSTRUCT
         |  CLONE a, b
         |  NEW (a)-[:KNOWS]->(b)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withRelationshipPropertyKeys("KNOWS")()
      .asCaps)
    result.getGraph.cypher("MATCH ()-[r]->() RETURN type(r)").getRecords.iterator.toBag should equal(Bag(
      CypherMap("type(r)" -> "KNOWS")
    ))
  }

  it("implictly clones when doing consecutive construction") {
    val query =
      """|CONSTRUCT
         |  NEW (a:A)-[r:FOO]->(b:B)
         |MATCH (a)-->(b)
         |CONSTRUCT
         |  NEW (a)-[:KNOWS]->(b)
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withRelationshipPropertyKeys("KNOWS")()
      .asCaps)
    result.getGraph.cypher("MATCH ()-[r]->() RETURN type(r)").getRecords.iterator.toBag should equal(Bag(
      CypherMap("type(r)" -> "KNOWS")
    ))
  }
}
