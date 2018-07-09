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
package org.opencypher.okapi.logical.impl.logical

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.{CyclicRelationship, DirectedRelationship, Pattern}
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.testing.{BaseTestSuite, MatchHelper}
import org.opencypher.okapi.testing.MatchHelper._
import org.scalatest.matchers._

import scala.language.implicitConversions

class LogicalPlannerTest extends BaseTestSuite with IrConstruction {

  val nodeA = IRField("a")(AnyNode)
  val nodeB = IRField("b")(AnyNode)
  val nodeG = IRField("g")(AnyNode)
  val relR = IRField("r")(AnyRelationship)


  val emptySqm = SolvedQueryModel.empty

  //  // Helper to create nicer expected results with `asCode`
  //  implicit val specialMappings = Map[Any, String](
  //    Schema.empty -> "Schema.empty",
  //    CTNode -> "CTNode",
  //    CTRelationship -> "CTRelationship",
  //    emptySqm -> "emptySqm",
  //    nodeA -> "nodeA",
  //    relR -> "relR",
  //    nodeB -> "nodeB",
  //    (nodeA: IRField) -> "nodeA",
  //    (relR: IRField) -> "relR",
  //    (nodeB: IRField) -> "nodeB"
  //  )

  test("convert load graph block") {
    val result = plan(irFor(leafBlock))
    val expected = Select(List.empty, leafPlan, emptySqm)
    result should equalWithTracing(expected)
  }

  test("convert match block") {
    val pattern = Pattern
      .empty[Expr]
      .withEntity(nodeA)
      .withEntity(nodeB)
      .withEntity(relR)
      .withConnection(relR, DirectedRelationship(nodeA, nodeB))

    val block = matchBlock(pattern)

    val scan1 = NodeScan(nodeA, leafPlan, emptySqm.withField(nodeA))
    val scan2 = NodeScan(nodeB, leafPlan, emptySqm.withField(nodeB))
    val ir = irFor(block)
    val result = plan(ir)

    val expected = Expand(nodeA, relR, nodeB, Directed, scan1, scan2, SolvedQueryModel(Set(nodeA, nodeB, relR)))

    result should equalWithoutResult(expected)
  }

  test("convert cyclic match block") {
    val pattern = Pattern
      .empty[Expr]
      .withEntity(nodeA)
      .withEntity(relR)
      .withConnection(relR, CyclicRelationship(nodeA))

    val block = matchBlock(pattern)
    val ir = irFor(block)

    val scan = NodeScan(nodeA, leafPlan, emptySqm.withField(nodeA))
    val expandInto = ExpandInto(nodeA, relR, nodeA, Directed, scan, SolvedQueryModel(Set(nodeA, relR)))

    plan(ir) should equalWithoutResult(expandInto)
  }

  test("convert project block") {
    val fields = Fields[Expr](Map(toField('a) -> Property('n, PropertyKey("prop"))(CTFloat)))
    val block = project(fields)

    val result = plan(irFor(block))

    val expected = Project(
      Property('n, PropertyKey("prop"))(CTFloat) -> Some('a), // n is a dangling reference here
      leafPlan,
      emptySqm.withFields('a))
    result should equalWithoutResult(expected)
  }

  test("plan query") {
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
      "foo" -> CypherString("test"))
    val result = plan(ir)

    val expected = Project(
      Property(Var("a")(CTNode("Administrator")), PropertyKey("name"))(CTNull) -> Some(Var("a.name")(CTNull)),
      Filter(
        Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTNull), Param("foo")(CTString))(
          CTBoolean),
        Project(
          Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTNull) -> None,
          Filter(
            HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
            Filter(
              HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
              Expand(
                Var("a")(AnyNode),
                Var("r")(AnyRelationship),
                Var("g")(AnyNode),
                Directed,
                NodeScan(
                  Var("a")(AnyNode),
                  Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
                  SolvedQueryModel(Set(nodeA), Set())
                ),
                NodeScan(
                  Var("g")(AnyNode),
                  Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
                  SolvedQueryModel(Set(IRField("g")(AnyNode)), Set())),
                SolvedQueryModel(Set(nodeA, IRField("g")(AnyNode), relR))
              ),
              SolvedQueryModel(
                Set(nodeA, IRField("g")(AnyNode), relR),
                Set(HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean)))
            ),
            SolvedQueryModel(
              Set(nodeA, IRField("g")(AnyNode), relR),
              Set(
                HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
                HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean))
            )
          ),
          SolvedQueryModel(
            Set(nodeA, IRField("g")(AnyNode), relR),
            Set(
              HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
              HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean))
          )
        ),
        SolvedQueryModel(
          Set(nodeA, IRField("g")(AnyNode), relR),
          Set(
            HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
            HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
            Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTNull), Param("foo")(CTString))(
              CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(nodeA, IRField("g")(AnyNode), relR, IRField("a.name")(CTNull)),
        Set(
          HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
          HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
          Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTNull), Param("foo")(CTString))(
            CTBoolean)
        )
      )
    )
    result should equalWithoutResult(expected)
  }

  test("plan query with type information") {
    implicit val schema: Schema = Schema.empty
      .withNodePropertyKeys("Group")("name" -> CTString)
      .withNodePropertyKeys("Administrator")("name" -> CTFloat)

    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
      "foo" -> CypherString("test"))

    val result = plan(ir, schema)

    val expected = Project(
      Property(Var("a")(CTNode("Administrator")), PropertyKey("name"))(CTFloat) -> Some(Var("a.name")(CTFloat)),
      Filter(
        Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString), Param("foo")(CTString))(
          CTBoolean),
        Project(
          Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString) -> None,
          Filter(
            HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
            Filter(
              HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
              Expand(
                Var("a")(AnyNode),
                Var("r")(AnyRelationship),
                Var("g")(AnyNode),
                Directed,
                NodeScan(
                  Var("a")(AnyNode),
                  Start(
                    LogicalCatalogGraph(
                      testQualifiedGraphName,
                      schema
                    ),
                    emptySqm
                  ),
                  SolvedQueryModel(Set(nodeA), Set())
                ),
                NodeScan(
                  Var("g")(AnyNode),
                  Start(
                    LogicalCatalogGraph(
                      testQualifiedGraphName,
                      schema
                    ),
                    emptySqm
                  ),
                  SolvedQueryModel(Set(IRField("g")(AnyNode)), Set())
                ),
                SolvedQueryModel(Set(nodeA, IRField("g")(AnyNode), relR), Set())
              ),
              SolvedQueryModel(
                Set(nodeA, IRField("g")(AnyNode), relR),
                Set(HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean)))
            ),
            SolvedQueryModel(
              Set(nodeA, IRField("g")(AnyNode), relR),
              Set(
                HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
                HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean))
            )
          ),
          SolvedQueryModel(
            Set(nodeA, IRField("g")(AnyNode), relR),
            Set(
              HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
              HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean))
          )
        ),
        SolvedQueryModel(
          Set(nodeA, IRField("g")(AnyNode), relR),
          Set(
            HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
            HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
            Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString), Param("foo")(CTString))(
              CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(nodeA, IRField("g")(AnyNode), relR, IRField("a.name")(CTFloat)),
        Set(
          HasLabel(Var("a")(AnyNode), Label("Administrator"))(CTBoolean),
          HasLabel(Var("g")(AnyNode), Label("Group"))(CTBoolean),
          Equals(Property(Var("g")(CTNode("Group")), PropertyKey("name"))(CTString), Param("foo")(CTString))(
            CTBoolean)
        )
      )
    )

    result should equalWithoutResult(expected)
  }

  test("plan query with negation") {
    val ir =
      "MATCH (a) WHERE NOT $p1 = $p2 RETURN a.prop".irWithParams("p1" -> CypherInteger(1L), "p2" -> CypherBoolean(true))

    val result = plan(ir)

    val expected = Project(
      Property(Var("a")(AnyNode), PropertyKey("prop"))(CTNull) -> Some(Var("a.prop")(CTNull)),
      Filter(
        Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean),
        NodeScan(
          Var("a")(AnyNode),
          Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
          SolvedQueryModel(Set(nodeA), Set())
        ),
        SolvedQueryModel(
          Set(nodeA),
          Set(Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean)))
      ),
      SolvedQueryModel(
        Set(nodeA, IRField("a.prop")(CTNull)),
        Set(Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean)))
    )

    result should equalWithoutResult(expected)
  }

  private val planner = new LogicalPlanner(new LogicalOperatorProducer)

  private def plan(ir: CypherStatement[Expr], schema: Schema = Schema.empty): LogicalOperator =
    plan(ir, schema, testGraphName -> schema)

  private def plan(ir: CypherStatement[Expr], ambientSchema: Schema, graphWithSchema: (GraphName, Schema)*): LogicalOperator = {
    val withAmbientGraph = graphWithSchema :+ (testGraphName -> ambientSchema)

    ir match {
      case cq: CypherQuery[Expr] =>
        planner.process(cq)(LogicalPlannerContext(ambientSchema, Set.empty, Map(testNamespace -> graphSource(withAmbientGraph: _*))))
      case _ => throw new IllegalArgumentException("Query is not a CypherQuery")
    }


  }

  case class equalWithoutResult(plan: LogicalOperator) extends Matcher[LogicalOperator] {
    override def apply(left: LogicalOperator): MatchResult = {
      left match {
        case Select(_, in, _) =>
          val planMatch = equalWithTracing(in)(plan)
          val solvedMatch = equalWithTracing(in.solved)(plan.solved)
          MatchHelper.combine(planMatch, solvedMatch)
        case _ => MatchResult(matches = false, "Expected a Select plan on top", "")
      }
    }
  }

  def graphSource(graphWithSchema: (GraphName, Schema)*): PropertyGraphDataSource = {
    import org.mockito.Mockito.when

    val graphSource = mock[PropertyGraphDataSource]
    graphWithSchema.foreach {
      case (graphName, schema) => when(graphSource.schema(graphName)).thenReturn(Some(schema))
    }

    graphSource
  }
}
