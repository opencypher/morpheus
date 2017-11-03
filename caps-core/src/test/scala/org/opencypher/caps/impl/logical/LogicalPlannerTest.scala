/*
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
package org.opencypher.caps.impl.logical

import java.net.URI

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.io.PersistMode
import org.opencypher.caps.api.record.{ProjectedExpr, ProjectedField}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.api.spark.io.CAPSGraphSource
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.{CypherBoolean, CypherInteger, CypherString}
import org.opencypher.caps.impl.logical
import org.opencypher.caps.impl.util.toVar
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block._
import org.opencypher.caps.ir.api.pattern.{
  DirectedRelationship,
  EveryNode,
  EveryRelationship,
  Pattern
}
import org.opencypher.caps.ir.impl.IrTestSuite
import org.opencypher.caps.toField
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.language.implicitConversions

class LogicalPlannerTest extends IrTestSuite {

  val nodeA = IRField("a")(CTNode)
  val nodeB = IRField("b")(CTNode)
  val nodeG = IRField("g")(CTNode)
  val relR  = IRField("r")(CTRelationship)

  test("convert load graph block") {
    plan(irFor(leafBlock)) should equal(Select(IndexedSeq.empty, Set.empty, leafPlan)(emptySqm))
  }

  test("convert match block") {
    val pattern = Pattern
      .empty[Expr]
      .withEntity(nodeA, EveryNode)
      .withEntity(nodeB, EveryNode)
      .withEntity(relR, EveryRelationship)
      .withConnection(relR, DirectedRelationship(nodeA, nodeB))

    val block = matchBlock(pattern)

    val scan1 = NodeScan(nodeA,
                         EveryNode,
                         SetSourceGraph(leafPlan.sourceGraph, leafPlan)(emptySqm.withField(nodeA)))(
      emptySqm.withField(nodeA))
    val scan2 = NodeScan(nodeB, EveryNode, leafPlan)(emptySqm.withField(nodeB))
    plan(irWithLeaf(block)) should equalWithoutResult(
      ExpandSource(nodeA, relR, EveryRelationship, nodeB, scan1, scan2)(
        emptySqm.withFields(nodeA, nodeB, relR))
    )
  }

  val emptySqm = SolvedQueryModel.empty[Expr]

  test("convert project block") {
    val fields =
      FieldsAndGraphs[Expr](Map(toField('a) -> Property('n, PropertyKey("prop"))(CTFloat)))
    val block = project(fields)

    plan(irWithLeaf(block)) should equalWithoutResult(
      Project(
        ProjectedField('a, Property('n, PropertyKey("prop"))(CTFloat)), // n is a dangling reference here
        leafPlan)(emptySqm.withFields('a))
    )
  }

  test("plan query") {
    val ir =
      "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
        "foo" -> CypherString("test"))

    plan(ir) should equal(
      Select(
        IndexedSeq(Var("a.name")(CTVoid)),
        Set.empty,
        Project(
          ProjectedField(Var("a.name")(CTVoid),
                         Property(Var("a")(CTNode("Administrator")), PropertyKey("name"))(CTVoid)),
          Filter(
            Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTVoid),
                   Param("foo")(CTString))(CTBoolean),
            Project(
              ProjectedExpr(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTVoid)),
              Filter(
                HasLabel(Var("g")(CTNode), Label("Group"))(CTBoolean),
                Filter(
                  HasLabel(Var("a")(CTNode), Label("Administrator"))(CTBoolean),
                  ExpandSource(
                    Var("a")(CTNode),
                    Var("r")(CTRelationship),
                    EveryRelationship,
                    Var("g")(CTNode),
                    NodeScan(
                      Var("a")(CTNode),
                      EveryNode,
                      SetSourceGraph(
                        LogicalExternalGraph(testGraph.name, testGraph.uri, Schema.empty),
                        Start(LogicalExternalGraph(testGraph.name, testGraph.uri, Schema.empty),
                              Set.empty)(emptySqm)
                      )(emptySqm)
                    )(emptySqm),
                    NodeScan(Var("g")(CTNode),
                             EveryNode,
                             Start(LogicalExternalGraph(testGraph.name,
                                                        testGraph.uri,
                                                        Schema.empty),
                                   Set.empty)(emptySqm))(emptySqm)
                  )(emptySqm)
                )(emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  test("plan query with type information") {
    implicit val schema: Schema = Schema.empty
      .withNodePropertyKeys("Group")("name" -> CTString)
      .withNodePropertyKeys("Administrator")("name" -> CTFloat)

    val ir =
      "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
        "foo" -> CypherString("test"))

    plan(ir, schema) should equal(
      Select(
        IndexedSeq(Var("a.name")(CTFloat)),
        Set.empty,
        Project(
          ProjectedField(Var("a.name")(CTFloat),
                         Property(Var("a")(CTNode("Administrator")), PropertyKey("name"))(CTFloat)),
          Filter(
            Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString),
                   Param("foo")(CTString))(CTBoolean),
            Project(
              ProjectedExpr(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString)),
              Filter(
                HasLabel(Var("g")(CTNode), Label("Group"))(CTBoolean),
                Filter(
                  HasLabel(Var("a")(CTNode), Label("Administrator"))(CTBoolean),
                  ExpandSource(
                    Var("a")(CTNode),
                    Var("r")(CTRelationship),
                    EveryRelationship,
                    Var("g")(CTNode),
                    NodeScan(
                      Var("a")(CTNode),
                      EveryNode,
                      SetSourceGraph(LogicalExternalGraph(testGraph.name, testGraph.uri, schema),
                                     Start(LogicalExternalGraph(testGraph.name,
                                                                testGraph.uri,
                                                                schema),
                                           Set.empty)(emptySqm))(emptySqm)
                    )(emptySqm),
                    NodeScan(Var("g")(CTNode),
                             EveryNode,
                             Start(LogicalExternalGraph(testGraph.name, testGraph.uri, schema),
                                   Set.empty)(emptySqm))(emptySqm)
                  )(emptySqm)
                )(emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  test("plan query with negation") {
    val ir = "MATCH (a) WHERE NOT $p1 = $p2 RETURN a.prop".irWithParams("p1" -> CypherInteger(1L),
                                                                        "p2" -> CypherBoolean(true))

    plan(ir) should equal(
      Select(
        IndexedSeq(Var("a.prop")(CTVoid)),
        Set.empty,
        Project(
          ProjectedField(Var("a.prop")(CTVoid), Property(nodeA, PropertyKey("prop"))(CTVoid)),
          Filter(
            Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean),
            NodeScan(
              nodeA,
              EveryNode,
              SetSourceGraph(
                LogicalExternalGraph(testGraph.name, testGraph.uri, Schema.empty),
                Start(LogicalExternalGraph(testGraph.name, testGraph.uri, Schema.empty), Set.empty)(
                  emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  test("do not project graphs multiple times") {
    val query = """
       |FROM GRAPH foo AT 'hdfs+csv://localhost/foo'
       |FROM GRAPH bar AT 'hdfs+csv://localhost/bar'
       |RETURN GRAPHS *
      """.stripMargin

    val ir = query.ir

    val startOp: LogicalOperator =
      Start(LogicalExternalGraph("test", URI.create("test"), Schema.empty), Set.empty)(emptySqm)
    val projectFoo: LogicalOperator =
      ProjectGraph(LogicalExternalGraph("foo", URI.create("test"), Schema.empty), startOp)(emptySqm)
    val projectBar: LogicalOperator = ProjectGraph(
      LogicalExternalGraph("bar", URI.create("test"), Schema.empty),
      projectFoo)(emptySqm)
    val select = Select(IndexedSeq.empty, Set("bar", "foo"), projectBar)(emptySqm)

    plan(ir) should equal(select)
  }

  private val planner = new LogicalPlanner(new LogicalOperatorProducer)

  private def plan(ir: CypherQuery[Expr], schema: Schema = Schema.empty) =
    planner.process(ir)(LogicalPlannerContext(schema, Set.empty, (_) => FakeGraphSource(schema)))

  case class equalWithoutResult(plan: LogicalOperator) extends Matcher[LogicalOperator] {
    override def apply(left: LogicalOperator): MatchResult = {
      left match {
        case logical.Select(_, _, in) =>
          val matches = in == plan && in.solved == plan.solved
          MatchResult(matches, s"$in did not equal $plan", s"$in was not supposed to equal $plan")
        case _ =>
          MatchResult(matches = false,
                      "Expected a Select plan on top",
                      "Expected a Select plan on top")
      }
    }
  }

  private case class FakeGraphSource(_schema: Schema) extends CAPSGraphSource {
    override lazy val session: Session                                 = ???
    override def canonicalURI: URI                                     = URI.create("test")
    override def sourceForGraphAt(uri: URI): Boolean                   = ???
    override def create: CAPSGraph                                     = ???
    override def graph: CAPSGraph                                      = ???
    override def schema: Option[Schema]                                = Some(_schema)
    override def store(graph: CAPSGraph, mode: PersistMode): CAPSGraph = ???
    override def delete(): Unit                                        = ???
  }
}
