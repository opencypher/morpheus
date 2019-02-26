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
package org.opencypher.okapi.logical.impl.logical

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.{CyclicRelationship, DirectedRelationship, Pattern}
import org.opencypher.okapi.ir.impl.QueryLocalCatalog
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.testing.MatchHelper._
import org.opencypher.okapi.testing.{BaseTestSuite, MatchHelper}
import org.scalatest.matchers._

import scala.language.implicitConversions

class LogicalPlannerTest extends BaseTestSuite with IrConstruction {

  val irFieldA: IRField = IRField("a")(CTNode("Administrator"))
  val irFieldB: IRField = IRField("b")(CTNode)
  val irFieldG: IRField = IRField("g")(CTNode("Group"))
  val irFieldR: IRField = IRField("r")(CTRelationship)

  val varA: Var = Var("a")(CTNode(Set("Administrator"), Some(testQualifiedGraphName)))
  val varB: Var = Var("b")(CTNode(Set.empty[String], Some(testQualifiedGraphName)))
  val varG: Var = Var("g")(CTNode(Set("Group"), Some(testQualifiedGraphName)))
  val varR: Var = Var("r")(CTRelationship(Set.empty[String], Some(testQualifiedGraphName)))

  val aLabelPredicate: HasLabel = HasLabel(varA, Label("Administrator"))(CTBoolean)

  val emptySqm: SolvedQueryModel = SolvedQueryModel.empty

  it("converts load graph block") {
    val result = plan(irFor(leafBlock))
    val expected = Select(List.empty, leafPlan, emptySqm)
    result should equalWithTracing(expected)
  }

  it("converts match block") {
    val pattern = Pattern
      .empty
      .withEntity(irFieldA)
      .withEntity(irFieldB)
      .withEntity(irFieldR)
      .withConnection(irFieldR, DirectedRelationship(irFieldA, irFieldB))

    val block = matchBlock(pattern)


    val scan1 = PatternScan.nodeScan(irFieldA, leafPlan, emptySqm.withField(irFieldA).withPredicate(aLabelPredicate))
    val scan2 = PatternScan.nodeScan(irFieldB, leafPlan, emptySqm.withField(irFieldB))
    val ir = irFor(block)
    val result = plan(ir)

    val expected = Expand(irFieldA, irFieldR, irFieldB, Outgoing, scan1, scan2, SolvedQueryModel(Set(irFieldA, irFieldB, irFieldR), Set(aLabelPredicate)))

    result should equalWithoutResult(expected)
  }

  it("converts cyclic match block") {
    val pattern = Pattern
      .empty
      .withEntity(irFieldA)
      .withEntity(irFieldR)
      .withConnection(irFieldR, CyclicRelationship(irFieldA))

    val block = matchBlock(pattern)
    val ir = irFor(block)

    val scan = PatternScan.nodeScan(irFieldA, leafPlan, emptySqm.withField(irFieldA).withPredicate(aLabelPredicate))
    val expandInto = ExpandInto(irFieldA, irFieldR, irFieldA, Outgoing, scan, SolvedQueryModel(Set(irFieldA, irFieldR), Set(aLabelPredicate)))

    plan(ir) should equalWithoutResult(expandInto)
  }

  it("converts project block") {
    val fields = Fields(Map(toField('a) -> Property('n, PropertyKey("prop"))(CTFloat)))
    val block = project(fields)

    val result = plan(irFor(block))

    val expected = Project(
      Property('n, PropertyKey("prop"))(CTFloat) -> Some('a), // n is a dangling reference here
      leafPlan,
      emptySqm.withFields('a))
    result should equalWithoutResult(expected)
  }

  it("plans query") {
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
      "foo" -> CypherString("test"))
    val result = plan(ir)

    val expected = Project(
      Property(varA, PropertyKey("name"))(CTNull) -> Some(Var("a.name")(CTNull)),
      Filter(
        Equals(Property(varG, PropertyKey("name"))(CTNull), Param("foo")(CTString))(CTBoolean),
        Expand(
          varA,
          varR,
          varG,
          Outgoing,
          PatternScan.nodeScan(
            varA,
            Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
            SolvedQueryModel(Set(irFieldA), Set(HasLabel(varA, Label("Administrator"))(CTBoolean)))
          ),
          PatternScan.nodeScan(
            varG,
            Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
            SolvedQueryModel(Set(irFieldG), Set(HasLabel(varG, Label("Group"))(CTBoolean)))
          ),
          SolvedQueryModel(
            Set(irFieldA, irFieldG, irFieldR),
            Set(
              HasLabel(varA, Label("Administrator"))(CTBoolean),
              HasLabel(varG, Label("Group"))(CTBoolean)
            )
          )
        ),
        SolvedQueryModel(
          Set(irFieldA, irFieldG, irFieldR),
          Set(
            HasLabel(varA, Label("Administrator"))(CTBoolean),
            HasLabel(varG, Label("Group"))(CTBoolean),
            Equals(Property(varG, PropertyKey("name"))(CTNull), Param("foo")(CTString))(
              CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(irFieldA, irFieldG, irFieldR, IRField("a.name")(CTNull)),
        Set(
          HasLabel(varA, Label("Administrator"))(CTBoolean),
          HasLabel(varG, Label("Group"))(CTBoolean),
          Equals(Property(varG, PropertyKey("name"))(CTNull), Param("foo")(CTString))(CTBoolean)
        )
      )
    )
    result should equalWithoutResult(expected)
  }

  it("plans query with type information") {
    implicit val schema: Schema = Schema.empty
      .withNodePropertyKeys("Group")("name" -> CTString)
      .withNodePropertyKeys("Administrator")("name" -> CTFloat)

    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = $foo RETURN a.name".irWithParams(
      "foo" -> CypherString("test"))

    val result = plan(ir, schema)

    val expected = Project(
      Property(Var("a")(CTNode(Set("Administrator"))), PropertyKey("name"))(CTFloat) -> Some(Var("a.name")(CTFloat)),
      Filter(
        Equals(Property(varG, PropertyKey("name"))(CTString), Param("foo")(CTString))(CTBoolean),
        Expand(
          varA,
          Var("r")(CTRelationship),
          varG,
          Outgoing,
          PatternScan.nodeScan(
            varA,
            Start(
              LogicalCatalogGraph(
                testQualifiedGraphName,
                schema
              ),
              emptySqm
            ),
            SolvedQueryModel(Set(irFieldA), Set(HasLabel(varA, Label("Administrator"))(CTBoolean)))
          ),
          PatternScan.nodeScan(
            varG,
            Start(
              LogicalCatalogGraph(
                testQualifiedGraphName,
                schema
              ),
              emptySqm
            ),
            SolvedQueryModel(Set(irFieldG), Set(HasLabel(varG, Label("Group"))(CTBoolean)))
          ),
          SolvedQueryModel(Set(irFieldA, irFieldG, irFieldR), Set(
            HasLabel(varA, Label("Administrator"))(CTBoolean),
            HasLabel(varG, Label("Group"))(CTBoolean)
          ))
        ),
        SolvedQueryModel(
          Set(irFieldA, irFieldG, irFieldR),
          Set(
            HasLabel(varA, Label("Administrator"))(CTBoolean),
            HasLabel(varG, Label("Group"))(CTBoolean),
            Equals(Property(varG, PropertyKey("name"))(CTString), Param("foo")(CTString))(
              CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(irFieldA, irFieldG, irFieldR, IRField("a.name")(CTFloat)),
        Set(
          HasLabel(varA, Label("Administrator"))(CTBoolean),
          HasLabel(varG, Label("Group"))(CTBoolean),
          Equals(Property(varG, PropertyKey("name"))(CTString), Param("foo")(CTString))(
            CTBoolean)
        )
      )
    )

    result should equalWithoutResult(expected)
  }

  it("plans query with negation") {
    val ir =
      "MATCH (a) WHERE NOT $p1 = $p2 RETURN a.prop".irWithParams("p1" -> CypherInteger(1L), "p2" -> CypherBoolean(true))

    val result = plan(ir)

    val varA2: Var = Var("a")(CTNode(Set.empty[String], Some(testQualifiedGraphName)))

    val expected = Project(
      Property(varA2, PropertyKey("prop"))(CTNull) -> Some(Var("a.prop")(CTNull)),
      Filter(
        Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean),
        PatternScan.nodeScan(
          varA2,
          Start(LogicalCatalogGraph(testQualifiedGraphName, Schema.empty), emptySqm),
          SolvedQueryModel(Set(irFieldA), Set())
        ),
        SolvedQueryModel(
          Set(irFieldA),
          Set(Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean)))
      ),
      SolvedQueryModel(
        Set(irFieldA, IRField("a.prop")(CTNull)),
        Set(Not(Equals(Param("p1")(CTInteger), Param("p2")(CTBoolean))(CTBoolean))(CTBoolean)))
    )
    result should equalWithoutResult(expected)
  }

  private val planner = new LogicalPlanner(new LogicalOperatorProducer)

  private def plan(ir: CypherStatement, schema: Schema = Schema.empty): LogicalOperator =
    plan(ir, schema, testGraphName -> schema)

  private def plan(
    ir: CypherStatement,
    ambientSchema: Schema,
    graphWithSchema: (GraphName, Schema)*
  ): LogicalOperator = {
    val withAmbientGraph = graphWithSchema :+ (testGraphName -> ambientSchema)

    ir match {
      case cq: SingleQuery =>
        planner.process(cq)(
          LogicalPlannerContext(
            ambientSchema,
            Set.empty,
            Map(testNamespace -> graphSource(withAmbientGraph: _*)),
            QueryLocalCatalog(
              Map(testNamespace -> graphSource(withAmbientGraph: _*))
            )
          )
        )
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
