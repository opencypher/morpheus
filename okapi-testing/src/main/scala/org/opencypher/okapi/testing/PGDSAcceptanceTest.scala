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
package org.opencypher.okapi.testing

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.GraphAlreadyExistsException
import org.opencypher.okapi.testing.Bag._
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, _}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

import scala.util.{Failure, Success, Try}

trait PGDSAcceptanceTest[Session <: CypherSession, Graph <: PropertyGraph] {
  self: BaseTestSuite =>

  object Scenario {
    def apply(name: String, initGraph: GraphName)(test: TestContext => Unit): Scenario = {
      Scenario(name, List(initGraph))(test)
    }
  }

  case class Scenario(
    override val name: String,
    initGraphs: List[GraphName] = Nil
  )(val test: TestContext => Unit) extends Tag(name)


  abstract class TestContextFactory {
    self =>

    val tag: Tag = new Tag(self.toString)

    def initializeContext(graphNames: List[GraphName]): TestContext = TestContext(initSession, initPgds(graphNames))

    def initPgds(graphNames: List[GraphName]): PropertyGraphDataSource

    def initSession: Session

    def releaseContext(implicit ctx: TestContext): Unit = {
      releasePgds
      releaseSession
    }

    def releasePgds(implicit ctx: TestContext): Unit = {
      pgds.graphNames.foreach(pgds.delete)
    }

    def releaseSession(implicit ctx: TestContext): Unit = {
      session.catalog.listSources.foreach { case (namespace, _) =>
        if (namespace != session.catalog.sessionNamespace) {
          session.deregisterSource(namespace)
        }
      }
    }
  }

  case class TestContext(session: Session, pgds: PropertyGraphDataSource)

  lazy val graph: Map[GraphName, Graph] = testCreateGraphStatements.mapValues(initGraph)

  def allScenarios: List[Scenario] = cypher10Scenarios

  def initGraph(createStatements: String): Graph

  def executeScenariosWithContext(scenarios: List[Scenario], contextFactory: TestContextFactory): Unit = {
    val scenarioTable = Table("Scenario", scenarios: _*)
    forAll(scenarioTable) { scenario =>
      test(s"[$contextFactory] ${scenario.name}", contextFactory.tag, scenario) {
        val ctx: TestContext = contextFactory.initializeContext(scenario.initGraphs)
        Try(scenario.test(ctx)) match {
          case Success(_) =>
            contextFactory.releaseContext(ctx)
          case Failure(ex) =>
            contextFactory.releaseContext(ctx)
            throw ex
        }
      }
    }
  }

  val ns = Namespace("testing")
  val g1 = GraphName("testGraph1")
  lazy val testCreateGraphStatements: Map[GraphName, String] = Map(
    g1 ->
      s"""
         |CREATE (a:A { name: 'A', date: date("2011-11-11") })
         |CREATE (b1:B { type: 'B1', datetime: localdatetime("2011-11-11T11:11:11") })
         |CREATE (b2:B { type: 'B2', size: 5 })
         |CREATE (combo1:A:B { name: 'COMBO1', type: 'AB1', size: 2 })
         |CREATE (combo2:A:B { name: 'COMBO2', type: 'AB2' })
         |CREATE (c:C { name: 'C' })
         |CREATE (ac:A:C { name: 'AC' })
         |CREATE (d { name: 'D', type: 'NO_LABEL' })
         |CREATE (a)-[:R { since: 2004 }]->(b1)
         |CREATE (b1)-[:R { since: 2005, before: false }]->(combo1)
         |CREATE (combo1)-[:S { since: 2006 }]->(combo1)
         |CREATE (ac)-[:T]->(combo2)
    """.stripMargin
  )

  def pgds()(implicit ctx: TestContext): PropertyGraphDataSource = ctx.pgds

  def session()(implicit ctx: TestContext): Session = ctx.session

  // TCK-style steps

  def registerPgds(namespace: Namespace)(implicit ctx: TestContext): Unit = {
    session.registerSource(namespace, ctx.pgds)
  }

  def executeQuery(query: String, parameters: CypherMap = CypherMap.empty)(implicit ctx: TestContext): Session#Result = {
    session.cypher(query, parameters)
  }

  def expectRecordsAnyOrder(result: Session#Result, expectedRecords: CypherMap*): Unit = {
    result.records.iterator.toBag should equal(
      expectedRecords.toBag
    )
  }

  val cypher10Scenarios: List[Scenario] = {
    List(
      Scenario("API: Session.registerSource", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.source(ns).hasGraph(g1) shouldBe true
        session.catalog.source(ns).hasGraph(GraphName("foo")) shouldBe false
      },

      Scenario("API: PropertyGraphDataSource.hasGraph", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.source(ns).hasGraph(g1) shouldBe true
        session.catalog.source(ns).hasGraph(GraphName("foo")) shouldBe false
      },

      Scenario("API: PropertyGraphDataSource.graphNames", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.source(ns).graphNames should contain(g1)
      },

      Scenario("API: PropertyGraphDataSource.graph", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.source(ns).graph(g1)
      },

      Scenario("API: Correct schema for graph #1", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        val expectedSchema = Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString, "date" -> CTDate)
          .withNodePropertyKeys("B")("type" -> CTString, "size" -> CTInteger.nullable, "datetime" -> CTLocalDateTime.nullable)
          .withNodePropertyKeys("A", "B")("name" -> CTString, "type" -> CTString, "size" -> CTInteger.nullable)
          .withNodePropertyKeys("C")("name" -> CTString)
          .withNodePropertyKeys("A", "C")("name" -> CTString)
          .withNodePropertyKeys()("name" -> CTString, "type" -> CTString)
          .withRelationshipPropertyKeys("R")("since" -> CTInteger, "before" -> CTBoolean.nullable)
          .withRelationshipPropertyKeys("S")("since" -> CTInteger)
          .withRelationshipPropertyKeys("T")()

        val schema = session.catalog.source(ns).schema(g1).getOrElse(session.catalog.source(ns).graph(g1).schema)
        schema.labelPropertyMap should equal(expectedSchema.labelPropertyMap)
        schema.relTypePropertyMap should equal(expectedSchema.relTypePropertyMap)
      },

      Scenario("API: PropertyGraphDataSource: correct node/rel count for graph #1", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.source(ns).graph(g1).nodes("n").size shouldBe 8
        val r = session.catalog.source(ns).graph(g1).relationships("r")
        session.catalog.source(ns).graph(g1).relationships("r").size shouldBe 4
      },

      Scenario("API: Cypher query directly on graph #1", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        session.catalog.graph(QualifiedGraphName(ns, g1)).cypher("MATCH (a:A) RETURN a.name").records.iterator.toBag should equal(Bag(
          CypherMap("a.name" -> "A"),
          CypherMap("a.name" -> "COMBO1"),
          CypherMap("a.name" -> "COMBO2"),
          CypherMap("a.name" -> "AC")
        ))
      },

      Scenario("Cypher query on session", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        expectRecordsAnyOrder(
          executeQuery(s"FROM GRAPH $ns.$g1 MATCH (b:B) RETURN b.type, b.size"),
          CypherMap("b.type" -> "B1", "b.size" -> CypherNull),
          CypherMap("b.type" -> "B2", "b.size" -> 5),
          CypherMap("b.type" -> "AB1", "b.size" -> 2),
          CypherMap("b.type" -> "AB2", "b.size" -> CypherNull)
        )
      },

      Scenario("Scans over multiple labels", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        expectRecordsAnyOrder(
          executeQuery(s"FROM GRAPH $ns.$g1 MATCH (n) RETURN n.name, n.size"),
          CypherMap("n.name" -> "A", "n.size" -> CypherNull),
          CypherMap("n.name" -> "C", "n.size" -> CypherNull),
          CypherMap("n.name" -> "AC", "n.size" -> CypherNull),
          CypherMap("n.name" -> "COMBO1", "n.size" -> 2),
          CypherMap("n.name" -> "COMBO2", "n.size" -> CypherNull),
          CypherMap("n.name" -> CypherNull, "n.size" -> 5),
          CypherMap("n.name" -> CypherNull, "n.size" -> CypherNull),
          CypherMap("n.name" -> "D", "n.size" -> CypherNull)
        )
      },

      Scenario("Multi-hop paths", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        expectRecordsAnyOrder(
          executeQuery(s"FROM GRAPH $ns.$g1 MATCH (a)-[r1]->(b)-[r2]->(c) RETURN r1.since, r2.since, type(r2)"),
          CypherMap("r1.since" -> 2004, "r2.since" -> 2005, "type(r2)" -> "R"),
          CypherMap("r1.since" -> 2005, "r2.since" -> 2006, "type(r2)" -> "S")
        )
      },

      Scenario("Initialize with a graph", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        val gn = GraphName("storedGraph")
        Try(session.cypher(s"CATALOG CREATE GRAPH $ns.$gn { FROM GRAPH $ns.$g1 RETURN GRAPH }")) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `true` after graph creation") {
              session.catalog.source(ns).hasGraph(gn) shouldBe true
            }
            session.catalog.graph(s"$ns.$gn").nodes("n").size shouldBe 8

            a[GraphAlreadyExistsException] shouldBe thrownBy {
              session.cypher(s"CATALOG CREATE GRAPH $ns.$g1 { RETURN GRAPH }")
            }
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Initialize with a constructed graph", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        val gn = GraphName("storedGraph")
        Try(session.cypher(
          s"""
             |CATALOG CREATE GRAPH $ns.$gn {
             |  CONSTRUCT ON $ns.$g1
             |    CREATE (c:C { name: 'new' })
             |  RETURN GRAPH
             |}
             |""".stripMargin)) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `true` after graph creation") {
              session.catalog.source(ns).hasGraph(gn) shouldBe true
            }
            val result = session.cypher(s"FROM GRAPH $ns.$gn MATCH (c:C) RETURN c.name").records.iterator.toBag
            result should equal(Bag(
              CypherMap("c.name" -> "C"),
              CypherMap("c.name" -> "AC"),
              CypherMap("c.name" -> "new")
            ))
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Initialize with nodes without labels") { implicit ctx: TestContext =>
        registerPgds(ns)
        val gn = GraphName("storedGraph")
        Try(session.cypher(
          s"""
             |CATALOG CREATE GRAPH $ns.$gn {
             |  CONSTRUCT
             |    CREATE ({ no_label_node: true })-[:SOME_REL_TYPE]->(:SOME_LABEL)
             |  RETURN GRAPH
             |}
             |""".stripMargin)) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `true` after graph creation") {
              session.catalog.source(ns).hasGraph(gn) shouldBe true
            }
            val result = session.cypher(
              s"FROM GRAPH $ns.$gn MATCH (d) WHERE size(labels(d))=0 RETURN d.no_label_node").records.iterator.toBag
            result should equal(Bag(
              CypherMap("d.no_label_node" -> true)
            ))
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Store European Latin unicode labels, rel types, property keys, and property values") { implicit
        ctx: TestContext =>
        registerPgds(ns)
        val gn = GraphName("storedGraph")
        Try(session.cypher(
          s"""
             |CATALOG CREATE GRAPH $ns.$gn {
             |  CONSTRUCT
             |    CREATE (:Āſ { Āſ: 'Āſ' })-[:Āſ]->(:ā)
             |  RETURN GRAPH
             |}
             |""".stripMargin)) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `true` after graph creation") {
              session.catalog.source(ns).hasGraph(gn) shouldBe true
            }
            val result = session.cypher(s"FROM GRAPH $ns.$gn MATCH (c:Āſ)-[:Āſ]-(:ā) RETURN c.Āſ").records.iterator.toBag
            result should equal(Bag(
              CypherMap("c.Āſ" -> "Āſ")
            ))
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Property with property key `id`") { implicit ctx: TestContext =>
        registerPgds(ns)
        val gn = GraphName("storedGraph")
        Try(session.cypher(
          s"""
             |CATALOG CREATE GRAPH $ns.$gn {
             |  CONSTRUCT
             |    CREATE ({ id: 100 })
             |  RETURN GRAPH
             |}
             |""".stripMargin)) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `true` after graph creation") {
              session.catalog.source(ns).hasGraph(GraphName(s"$gn")) shouldBe true
            }
            val result = session.cypher(s"FROM GRAPH $ns.$gn MATCH (c) RETURN c.id").records.iterator.toBag
            result should equal(Bag(
              CypherMap("c.id" -> 100)
            ))
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Store a union graph") { implicit ctx: TestContext =>
        registerPgds(ns)
        session.cypher("CATALOG CREATE GRAPH g1 { CONSTRUCT CREATE () RETURN GRAPH }")
        session.cypher("CATALOG CREATE GRAPH g2 { CONSTRUCT CREATE () RETURN GRAPH }")
        val unionGraphName = GraphName("union")

        val g1 = session.catalog.graph("g1")
        val g2 = session.catalog.graph("g2")

        g1.nodes("n").size shouldBe 1
        g2.nodes("n").size shouldBe 1

        val unionGraph = g1.unionAll(g2)
        unionGraph.nodes("n").size shouldBe 2

        Try {
          session.catalog.source(ns).store(unionGraphName, unionGraph)
        } match {
          case Success(_) =>
            withClue("`graph` needs to return graph with correct node size after storing a union graph") {
              session.catalog.source(ns).graph(unionGraphName).nodes("n").size shouldBe 2
            }
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      },

      Scenario("Repeat CONSTRUCT ON", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        val firstConstructedGraphName = GraphName("first")
        val secondConstructedGraphName = GraphName("second")
        val graph = session.catalog.source(ns).graph(g1)
        graph.nodes("n").size shouldBe 8
        val firstConstructedGraph = graph.cypher(
          s"""
             |CONSTRUCT
             |  ON $ns.$g1
             |  CREATE (:A {name: "A"})
             |  RETURN GRAPH
            """.stripMargin).graph
        firstConstructedGraph.nodes("n").size shouldBe 9
        val maybeStored = Try(session.catalog.source(ns).store(firstConstructedGraphName, firstConstructedGraph))
        maybeStored match {
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
          case Success(_) =>
            val retrievedConstructedGraph = session.catalog.source(ns).graph(firstConstructedGraphName)
            retrievedConstructedGraph.nodes("n").size shouldBe 9
            val secondConstructedGraph = graph.cypher(
              s"""
                 |CONSTRUCT
                 |  ON $ns.$firstConstructedGraphName
                 |  CREATE (:A:B {name: "COMBO", size: 2})
                 |  RETURN GRAPH
            """.stripMargin).graph
            secondConstructedGraph.nodes("n").size shouldBe 10
            session.catalog.source(ns).store(secondConstructedGraphName, secondConstructedGraph)
            val retrievedSecondConstructedGraph = session.catalog.source(ns).graph(secondConstructedGraphName)
            retrievedSecondConstructedGraph.nodes("n").size shouldBe 10
        }
      },

      Scenario("Drop a graph", g1) { implicit ctx: TestContext =>
        registerPgds(ns)
        Try(session.cypher(s"CATALOG DROP GRAPH $ns.$g1")) match {
          case Success(_) =>
            withClue("`hasGraph` needs to return `false` after graph deletion") {
              session.catalog.source(ns).hasGraph(g1) shouldBe false
            }
          case Failure(_: UnsupportedOperationException) =>
          case Failure(t) => throw t
        }
      }
    )
  }

}
