package org.opencypher.okapi.testing

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull}
import org.opencypher.okapi.impl.exception.GraphNotFoundException
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
      |CREATE (a)-[r:R { since: 2004 }]->(b)
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

  it("supports queries through the API") {
    val g = cypherSession.graph(QualifiedGraphName(ns, gn))

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

  it("stores a graph") {
    Try(cypherSession.cypher(s"CREATE GRAPH $ns.${gn}2 { FROM GRAPH $ns.$gn RETURN GRAPH }")) match {
      case Success(_) =>
        withClue("`hasGraph` needs to return `true` after graph creation") {
          cypherSession.dataSource(ns).hasGraph(GraphName(s"${gn}2")) shouldBe true
        }
      case Failure(_: UnsupportedOperationException) =>
      case other => fail(s"Expected success or `UnsupportedOperationException`, got $other")
    }
  }

  it("deletes a graph") {
    Try(cypherSession.cypher(s"DELETE GRAPH $ns.$gn")) match {
      case Success(_) =>
        withClue("`hasGraph` needs to return `false` after graph deletion") {
          cypherSession.dataSource(ns).hasGraph(gn) shouldBe false
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
