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
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.{GraphAlreadyExistsException, GraphNotFoundException}
import org.opencypher.okapi.testing.Bag._
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll, _}

import scala.util.{Failure, Success, Try}

trait Cypher10Acceptance[Session <: CypherSession, Graph <: PropertyGraph] {
  self: BaseTestSuite =>

  lazy val graph: Map[GraphName, Graph] = testCreateGraphStatements.mapValues(initGraph)

  def allScenarios: List[Cypher10Scenario] = cypher10Scenarios

  def initSession: Session

  def initGraph(createStatements: String): Graph

  def initPgds: PropertyGraphDataSource

  def returnPgds(pgds: PropertyGraphDataSource): Unit = {
    pgds.graphNames.foreach(pgds.delete)
  }

  def returnContext(implicit ctx: TestContext): Unit = {
    returnSession(session)
    returnPgds(pgds)
  }

  def returnSession(session: Session): Unit = {
    session.catalog.listSources.foreach { case (namespace, _) =>
      if (namespace != session.catalog.sessionNamespace) {
        session.deregisterSource(namespace)
      }
    }
  }

  def initContext: TestContext = TestContext(initSession, initPgds)

  def execute(scenarios: List[Cypher10Scenario]): Unit = {
    val testTag = new Tag(getClass.getSimpleName)
    val scenarioTable = Table("Read-only scenario", scenarios: _*)
    forAll(scenarioTable) { scenario =>
      test(s"[${testTag.name}] ${scenario.name}", testTag, scenario) {
        val ctx: TestContext = initContext
        Try(scenario.test(ctx)) match {
          case Success(_) =>
            returnContext(ctx)
          case Failure(ex) =>
            returnContext(ctx)
            throw ex
        }
      }
    }
  }

  case class TestContext(session: Session, pgds: PropertyGraphDataSource)

  case class Cypher10Scenario(
    override val name: String
  )(val test: TestContext => Unit) extends Tag(name)

  val ns = Namespace("testing")
  val g1 = GraphName("testGraph1")
  lazy val testCreateGraphStatements: Map[GraphName, String] = Map(
    g1 ->
      s"""
         |CREATE (a:A { name: 'A' })
         |CREATE (b1:B { type: 'B1' })
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

  def registerPgdsStep(namespace: Namespace)(implicit ctx: TestContext): Unit = {
    session.registerSource(namespace, ctx.pgds)
  }

  def storeGraphStep(gn: GraphName)(implicit ctx: TestContext): Unit = {
    pgds.store(gn, graph(gn))
  }

  def executeStep(query: String, parameters: CypherMap = CypherMap.empty)(implicit ctx: TestContext): Session#Result = {
    session.cypher(query, parameters)
  }

  def expectRecordsAnyOrderStep(result: Session#Result, expectedRecords: CypherMap*): Unit = {
    result.records.iterator.toBag should equal(
      expectedRecords.toBag
    )
  }

  // TODO: Extend TCK with Cypher 10 tests and implement with updated TCK
  lazy val cypher10Scenarios: List[Cypher10Scenario] = {
    List(
      Cypher10Scenario("API: Session.registerSource") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.source(ns).hasGraph(g1) shouldBe true
        session.catalog.source(ns).hasGraph(GraphName("foo")) shouldBe false
      },

      Cypher10Scenario("API: PropertyGraphDataSource.hasGraph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.source(ns).hasGraph(g1) shouldBe true
        session.catalog.source(ns).hasGraph(GraphName("foo")) shouldBe false
      },

      Cypher10Scenario("API: PropertyGraphDataSource.graphNames") { implicit ctx: TestContext =>
        pgds.graphNames should be(Set.empty)
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.source(ns).graphNames should be(Set(g1))
      },

      Cypher10Scenario("API: PropertyGraphDataSource.graph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.source(ns).graph(g1)
      },

      Cypher10Scenario("API: Correct schema for graph #1") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        val expectedSchema = Schema.empty
          .withNodePropertyKeys("A")("name" -> CTString)
          .withNodePropertyKeys("B")("type" -> CTString, "size" -> CTInteger.nullable)
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

      Cypher10Scenario("API: PropertyGraphDataSource: correct node/rel count for graph #1") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.source(ns).graph(g1).nodes("n").size shouldBe 8
        session.catalog.source(ns).graph(g1).relationships("r").size shouldBe 4
      },

      Cypher10Scenario("API: Cypher query directly on graph #1") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        session.catalog.graph(QualifiedGraphName(ns, g1)).cypher("MATCH (a:A) RETURN a.name").records.iterator.toBag should equal(Bag(
          CypherMap("a.name" -> "A"),
          CypherMap("a.name" -> "COMBO1"),
          CypherMap("a.name" -> "COMBO2"),
          CypherMap("a.name" -> "AC")
        ))
      },

      Cypher10Scenario("Cypher query on session") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        expectRecordsAnyOrderStep(
          executeStep(s"FROM GRAPH $ns.$g1 MATCH (b:B) RETURN b.type, b.size"),
          CypherMap("b.type" -> "B1", "b.size" -> CypherNull),
          CypherMap("b.type" -> "B2", "b.size" -> 5),
          CypherMap("b.type" -> "AB1", "b.size" -> 2),
          CypherMap("b.type" -> "AB2", "b.size" -> CypherNull)
        )
      },

      Cypher10Scenario("Scans over multiple labels") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        expectRecordsAnyOrderStep(
          executeStep(s"FROM GRAPH $ns.$g1 MATCH (n) RETURN n.name, n.size"),
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

      Cypher10Scenario("multi-hop paths") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
        expectRecordsAnyOrderStep(
          executeStep(s"FROM GRAPH $ns.$g1 MATCH (a)-[r1]->(b)-[r2]->(c) RETURN r1.since, r2.since, type(r2)"),
          CypherMap("r1.since" -> 2004, "r2.since" -> 2005, "type(r2)" -> "R"),
          CypherMap("r1.since" -> 2005, "r2.since" -> 2006, "type(r2)" -> "S")
        )
      },

      Cypher10Scenario("store a graph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
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

      Cypher10Scenario("store a constructed graph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
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

      Cypher10Scenario("store nodes without labels") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
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

      Cypher10Scenario("store European Latin unicode labels, rel types, property keys, and property values") { implicit
        ctx: TestContext =>
        registerPgdsStep(ns)
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

      Cypher10Scenario("property with property key `id`") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
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

      Cypher10Scenario("store a union graph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
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

      Cypher10Scenario("repeat CONSTRUCT ON") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
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

      Cypher10Scenario("drop a graph") { implicit ctx: TestContext =>
        registerPgdsStep(ns)
        storeGraphStep(g1)
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

case class SingleGraphDataSource(graphName: GraphName, graph: PropertyGraph) extends PropertyGraphDataSource {

  override def hasGraph(name: GraphName): Boolean = {
    name == graphName
  }

  override def graph(name: GraphName): PropertyGraph = {
    if (name == graphName) graph else throw GraphNotFoundException(s"Graph $name not found")
  }

  override def schema(name: GraphName): Option[Schema] = ???

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
