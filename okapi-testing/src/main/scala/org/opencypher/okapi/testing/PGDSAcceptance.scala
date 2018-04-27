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
package org.opencypher.okapi.testing

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.GraphNotFoundException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.okapi.testing.Bag._
import org.opencypher.okapi.testing.propertygraph.{TestGraph, TestGraphFactory}
import org.scalatest.BeforeAndAfterAll

import scala.util.{Failure, Success, Try}

trait PGDSAcceptance extends BeforeAndAfterAll {
  self: BaseTestSuite =>

  val createStatements =
    """
      |CREATE (a:A { name: 'A' })
      |CREATE (b:B { name: 'B' })
      |CREATE (combo:A:B { name: 'COMBO', size: 2 })
      |CREATE (a)-[:R { since: 2004 }]->(b)
      |CREATE (b)-[:R { since: 2005 }]->(combo)
      |CREATE (combo)-[:S { since: 2006 }]->(combo)
    """.stripMargin

  lazy val testGraph = TestGraphFactory(createStatements)

  val ns = Namespace("testing")
  val gn = GraphName("test")

  implicit val cypherSession: CypherSession = initSession()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val ds = create(gn, testGraph, createStatements)
    cypherSession.registerSource(ns, ds)
  }

  override def afterAll(): Unit = super.afterAll()

  def initSession(): CypherSession

  def create(graphName: GraphName, testGraph: TestGraph, createStatements: String): PropertyGraphDataSource

  it("supports `hasGraph`") {
    cypherSession.catalog.source(ns).hasGraph(gn) shouldBe true
    cypherSession.catalog.source(ns).hasGraph(GraphName("foo")) shouldBe false
  }

  it("supports `graph`") {
    cypherSession.catalog.source(ns).graph(gn).nodes("n").size shouldBe 3
    intercept[GraphNotFoundException] {
      cypherSession.catalog.source(ns).graph(GraphName("foo"))
    }
  }

  it("supports `graphNames`") {
    val graphNames = cypherSession.catalog.source(ns).graphNames
    graphNames.size shouldBe 1
    graphNames.head shouldBe gn
  }

  it("supports schema") {
    val schema: Schema = SchemaImpl(
      LabelPropertyMap(Map(Set("A") -> Map("name" -> CTString), Set("B") -> Map("name" -> CTString), Set("A", "B") -> Map("size" -> CTInteger, "name" -> CTString))),
      RelTypePropertyMap(Map("R" -> Map("since" -> CTInteger), "S" -> Map("since" -> CTInteger)))
    )

    cypherSession.catalog.source(ns).schema(gn) match {
      case Some(s) =>
        s.labelPropertyMap should equal(schema.labelPropertyMap)
        s.relTypePropertyMap should equal(schema.relTypePropertyMap)
      case None =>
        val s = cypherSession.catalog.source(ns).graph(gn).schema
        s.labelPropertyMap should equal(schema.labelPropertyMap)
        s.relTypePropertyMap should equal(schema.relTypePropertyMap)
    }
  }

  it("supports queries through the API") {
    val g = cypherSession.catalog.graph(QualifiedGraphName(ns, gn))

    g.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "A"),
      CypherMap("a.name" -> "COMBO")
    ))
  }

  it("supports queries through Cypher") {
    cypherSession.cypher(s"FROM GRAPH $ns.$gn MATCH (a:B) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "B"),
      CypherMap("a.name" -> "COMBO")
    ))
  }

  it("supports scans over multiple labels") {
    cypherSession.cypher(s"FROM GRAPH $ns.$gn MATCH (a) RETURN a.name, a.size").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "A", "a.size" -> CypherNull),
      CypherMap("a.name" -> "B", "a.size" -> CypherNull),
      CypherMap("a.name" -> "COMBO", "a.size" -> 2)
    ))
  }

  it("supports multi-hop paths") {
    cypherSession.cypher(s"FROM GRAPH $ns.$gn MATCH (a)-[r1]->(b)-[r2]->(c) RETURN r1.since, r2.since, type(r2)").getRecords.iterator.toBag should equal(Bag(
      CypherMap("r1.since" -> 2004, "r2.since" -> 2005, "type(r2)" -> "R"),
      CypherMap("r1.since" -> 2005, "r2.since" -> 2006, "type(r2)" -> "S")
    ))
  }

  it("stores a graph") {
    Try(cypherSession.cypher(s"CREATE GRAPH $ns.${gn}2 { FROM GRAPH $ns.$gn RETURN GRAPH }")) match {
      case Success(_) =>
        withClue("`hasGraph` needs to return `true` after graph creation") {
          cypherSession.catalog.source(ns).hasGraph(GraphName(s"${gn}2")) shouldBe true
        }
      case Failure(_: UnsupportedOperationException) =>
      case other => fail(s"Expected success or `UnsupportedOperationException`, got $other")
    }
  }

  it("stores a constructed graph") {
    Try(cypherSession.cypher(
      s"""
         |CREATE GRAPH $ns.${gn}3 {
         |  CONSTRUCT ON $ns.$gn
         |    NEW (c: C { name: 'C' })
         |  RETURN GRAPH
         |}
         |""".stripMargin)) match {
      case Success(_) =>
        withClue("`hasGraph` needs to return `true` after graph creation") {
          cypherSession.catalog.source(ns).hasGraph(GraphName(s"${gn}3")) shouldBe true
        }
        val result = cypherSession.cypher(s"FROM GRAPH $ns.${gn}3 MATCH (c:C) RETURN c.name").getRecords.iterator.toBag
        result should equal(Bag(
          CypherMap("c.name" -> "C")
        ))
      case Failure(_: UnsupportedOperationException) =>
      case other => fail(s"Expected success or `UnsupportedOperationException`, got $other")
    }
  }

  // TODO: Requires fixing https://github.com/opencypher/cypher-for-apache-spark/issues/402
  ignore("supports UNION ALl (requires storing/loading graph tags for CAPS)") {
    val firstUnionGraphName = GraphName("first")
    val secondUnionGraphName = GraphName("second")

    val graph = cypherSession.catalog.source(ns).graph(gn)
    graph.nodes("n").size shouldBe 3

    val firstUnionGraph = graph.unionAll(graph)
    firstUnionGraph.nodes("n").size shouldBe 6

    cypherSession.catalog.source(ns).store(firstUnionGraphName, firstUnionGraph)
    val retrievedUnionGraph = cypherSession.catalog.source(ns).graph(firstUnionGraphName)
    retrievedUnionGraph.nodes("n").size shouldBe 6

    val secondUnionGraph = retrievedUnionGraph.unionAll(graph)
    secondUnionGraph.nodes("n").size shouldBe 9

    cypherSession.catalog.source(ns).store(firstUnionGraphName, firstUnionGraph)
    val retrievedSecondUnionGraph = cypherSession.catalog.source(ns).graph(secondUnionGraphName)
    retrievedSecondUnionGraph.nodes("n").size shouldBe 9
  }

  // TODO: https://github.com/opencypher/cypher-for-apache-spark/issues/408
  // TODO: https://github.com/opencypher/cypher-for-apache-spark/issues/409
  ignore("supports repeated CONSTRUCT ON (requires storing/loading graph tags for CAPS)") {
    val firstConstructedGraphName = GraphName("first")
    val secondConstructedGraphName = GraphName("second")
    val graph = cypherSession.catalog.source(ns).graph(gn)
    graph.nodes("n").size shouldBe 3
    val firstConstructedGraph = graph.cypher(
      s"""
         |CONSTRUCT
         |  ON $ns.$gn
         |  NEW (:A {name: "A"})
         |  RETURN GRAPH
        """.stripMargin).getGraph
    firstConstructedGraph.nodes("n").size shouldBe 4
    val maybeStored = Try(cypherSession.catalog.source(ns).store(firstConstructedGraphName, firstConstructedGraph))
    maybeStored match {
      case Failure(_: UnsupportedOperationException) =>
      case Failure(f) => throw new Exception(s"Expected either an `UnsupportedOperationException` or a successful store", f)
      case Success(_) =>
        val retrievedConstructedGraph = cypherSession.catalog.source(ns).graph(firstConstructedGraphName)
        retrievedConstructedGraph.nodes("n").size shouldBe 4
        val secondConstructedGraph = graph.cypher(
          s"""
             |CONSTRUCT
             |  ON $ns.$firstConstructedGraphName
             |  NEW (:A:B {name: "COMBO", size: 2})
             |  RETURN GRAPH
        """.stripMargin).getGraph
        secondConstructedGraph.nodes("n").size shouldBe 5
        cypherSession.catalog.source(ns).store(firstConstructedGraphName, secondConstructedGraph)
        val retrievedSecondConstructedGraph = cypherSession.catalog.source(ns).graph(secondConstructedGraphName)
        retrievedSecondConstructedGraph.nodes("n").size shouldBe 5
    }
  }

  it("deletes a graph") {
    Try(cypherSession.cypher(s"DELETE GRAPH $ns.$gn")) match {
      case Success(_) =>
        withClue("`hasGraph` needs to return `false` after graph deletion") {
          cypherSession.catalog.source(ns).hasGraph(gn) shouldBe false
        }
      case Failure(_: UnsupportedOperationException) =>
      case other => fail(s"Expected success or `UnsupportedOperationException`, got $other")
    }
  }

}

case class SingleGraphDataSource(graphName: GraphName, graph: PropertyGraph) extends PropertyGraphDataSource {

  override def hasGraph(name: GraphName): Boolean = {
    name == graphName
  }

  override def graph(name: GraphName): PropertyGraph = {
    if (name == graphName) graph else throw new GraphNotFoundException(s"Graph $name not found")
  }

  override def schema(name: GraphName): Option[Schema] = ???

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
