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
import org.scalatest.BeforeAndAfterEach

import scala.util.{Failure, Success, Try}

trait PGDSAcceptance[Session <: CypherSession] extends BeforeAndAfterEach {
  self: BaseTestSuite =>

  val createStatements: String =
    """
      |CREATE (a:A { name: 'A' })
      |CREATE (b:B { prop: 'B' })
      |CREATE (combo:A:B { name: 'COMBO', size: 2 })
      |CREATE (a)-[:R { since: 2004 }]->(b)
      |CREATE (b)-[:R { since: 2005 }]->(combo)
      |CREATE (combo)-[:S { since: 2006 }]->(combo)
    """.stripMargin

  lazy val testGraph = TestGraphFactory(createStatements)

  val ns = Namespace("testing")
  val gn = GraphName("test")

  val cypherSession: Session = initSession()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val ds = create(gn, testGraph, createStatements)
    cypherSession.registerSource(ns, ds)
  }

  override protected def afterEach(): Unit = {
    cypherSession.deregisterSource(ns)
    super.afterEach()
  }

  def initSession(): Session

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
      LabelPropertyMap(Map(Set("A") -> Map("name" -> CTString), Set("B") -> Map("prop" -> CTString), Set("A", "B") -> Map("size" -> CTInteger, "name" -> CTString))),
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
    cypherSession.cypher(s"FROM GRAPH $ns.$gn MATCH (b:B) RETURN b.prop").getRecords.iterator.toBag should equal(Bag(
      CypherMap("b.prop" -> "B"),
      CypherMap("b.prop" -> CypherNull)
    ))
  }

  it("supports scans over multiple labels") {
    cypherSession.cypher(s"FROM GRAPH $ns.$gn MATCH (a) RETURN a.name, a.size").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "A", "a.size" -> CypherNull),
      CypherMap("a.name" -> CypherNull, "a.size" -> CypherNull),
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
      case Failure(t) => badFailure(t)
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
      case Failure(t) => badFailure(t)
    }
  }

  it("supports storing a union graph") {
    cypherSession.cypher("CREATE GRAPH g1 { CONSTRUCT NEW () RETURN GRAPH }")
    cypherSession.cypher("CREATE GRAPH g2 { CONSTRUCT NEW () RETURN GRAPH }")
    val unionGraphName = GraphName("union")

    val g1 = cypherSession.catalog.graph("g1")
    val g2 = cypherSession.catalog.graph("g2")

    g1.nodes("n").size shouldBe 1
    g2.nodes("n").size shouldBe 1

    val unionGraph = g1.unionAll(g2)
    unionGraph.nodes("n").size shouldBe 2

    Try {
      cypherSession.catalog.source(ns).store(unionGraphName, unionGraph)
    } match {
      case Success(_) =>
        withClue("`graph` needs to return graph with correct node size after storing a union graph") {
          cypherSession.catalog.source(ns).graph(unionGraphName).nodes("n").size shouldBe 2
        }
      case Failure(_: UnsupportedOperationException) =>
      case Failure(t) => badFailure(t)
    }
  }

  it("supports repeated CONSTRUCT ON") {
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
      case Failure(f) => badFailure(f)
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
        cypherSession.catalog.source(ns).store(secondConstructedGraphName, secondConstructedGraph)
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
      case Failure(t) => badFailure(t)
    }
  }

  protected def badFailure(t: Throwable): Unit = {
    fail(s"Expected success or `UnsupportedOperationException`, got $t")
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
