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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, _}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper._
import org.opencypher.okapi.trees.BottomUp

import scala.language.implicitConversions

class LogicalOptimizerTest extends BaseTestSuite with IrConstruction {

  val emptySqm = SolvedQueryModel.empty
  val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, Schema.empty)
  val schema = Schema.empty

  //  //Helper to create nicer expected results with `asCode`
  //  import org.opencypher.caps.impl.common.AsCode._
  //  implicit val specialMappings = Map[Any, String](
  //    schema -> "schema",
  //    emptySqm -> "emptySqm",
  //    logicalGraph -> "logicalGraph",
  //    emptySqm -> "emptySqm",
  //    (CTNode: CTNode) -> "CTNode"
  //  )

  def plannerContext(schema: Schema) =
    LogicalPlannerContext(schema, Set.empty, Map(testNamespace -> testGraphSource(testGraphName -> schema)))

  it("pushes label filter into scan") {
    val animalSchema = schema.withNodePropertyKeys("Animal")()
    val animalGraph = LogicalCatalogGraph(testQualifiedGraphName, animalSchema)
    val query =
      """|MATCH (a:Animal)
         |RETURN a""".stripMargin
    val plan = logicalPlan(query, animalSchema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(animalSchema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal")))),
      NodeScan(
        Var("a")(CTNode(Set("Animal"))),
        Start(
          animalGraph,
          emptySqm
        ),
        SolvedQueryModel(Set(), Set(HasLabel(Var("a")(CTNode(Set("Animal"))), Label("Animal"))(CTBoolean)))
      ),
      SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  it("rewrites missing label scan to empty records") {
    val query =
      """|MATCH (a:Animal)
         |RETURN a""".stripMargin
    val plan = logicalPlan(query, schema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal")))),
      EmptyRecords(
        Set(Var("a")(CTNode(Set("Animal")))),
        Start(logicalGraph, emptySqm),
        SolvedQueryModel(Set(), Set(HasLabel(Var("a")(CTNode(Set("Animal"))), Label("Animal"))(CTBoolean)))
      ),
      SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  it("rewrites missing label combination") {
    val query =
      """|MATCH (a:Animal:Astronaut)
         |RETURN a""".stripMargin
    val schema = Schema.empty.withNodePropertyKeys("Animal")().withNodePropertyKeys("Astronaut")()
    val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, schema)

    val plan = logicalPlan(query, schema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal", "Astronaut")))),
      EmptyRecords(
        Set(Var("a")(CTNode(Set("Astronaut", "Animal")))),
        Start(logicalGraph, emptySqm),
        SolvedQueryModel(
          Set(),
          Set(
            HasLabel(Var("a")(CTNode(Set("Astronaut", "Animal"))), Label("Astronaut"))(CTBoolean),
            HasLabel(Var("a")(CTNode(Set("Astronaut", "Animal"))), Label("Animal"))(CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(IRField("a")(CTNode)),
        Set(
          HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean),
          HasLabel(Var("a")(CTNode), Label("Astronaut"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  describe("replace cartesian with ValueJoin") {

    it("should replace cross with value join if filter is present") {
      val startA = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val startB = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val varA = Var("a")(CTNode)
      val propA = expr.Property(varA, PropertyKey("name"))(CTString)
      val varB = Var("b")(CTNode)
      val propB = expr.Property(varB, PropertyKey("name"))(CTString)
      val equals = Equals(propA, propB)(CTBoolean)
      val irFieldA = IRField(varA.name)(varA.cypherType)
      val irFieldB = IRField(varB.name)(varB.cypherType)

      val scanA = NodeScan(varA, startA, SolvedQueryModel(Set(irFieldA)))
      val scanB = NodeScan(varB, startB, SolvedQueryModel(Set(irFieldB)))
      val cartesian = CartesianProduct(scanA, scanB, SolvedQueryModel(Set(irFieldA, irFieldB)))
      val filter = Filter(equals, cartesian, SolvedQueryModel(Set(irFieldA, irFieldB)))

      val optimizedPlan = BottomUp[LogicalOperator](LogicalOptimizer.replaceCartesianWithValueJoin).transform(filter)

      val projectA = Project(propA -> None, scanA, scanA.solved)
      val projectB = Project(propB -> None, scanB, scanB.solved)
      val solved = SolvedQueryModel(Set(irFieldA, irFieldB)).withPredicate(equals)
      val valueJoin = ValueJoin(projectA, projectB, Set(equals), solved)

      optimizedPlan should equalWithTracing(valueJoin)
    }

    it("should replace cross with value join if filter with flipped predicate is present") {
      val startA = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val startB = Start(LogicalCatalogGraph(testQualifiedGraphName, testGraphSchema), SolvedQueryModel.empty)
      val varA = Var("a")(CTNode)
      val propA = expr.Property(varA, PropertyKey("name"))(CTString)
      val varB = Var("b")(CTNode)
      val propB = expr.Property(varB, PropertyKey("name"))(CTString)
      val equals = Equals(propB, propA)(CTBoolean)
      val irFieldA = IRField(varA.name)(varA.cypherType)
      val irFieldB = IRField(varB.name)(varB.cypherType)

      val scanA = NodeScan(varA, startA, SolvedQueryModel(Set(irFieldA)))
      val scanB = NodeScan(varB, startB, SolvedQueryModel(Set(irFieldB)))
      val cartesian = CartesianProduct(scanA, scanB, SolvedQueryModel(Set(irFieldA, irFieldB)))
      val filter = Filter(equals, cartesian, SolvedQueryModel(Set(irFieldA, irFieldB)))

      val optimizedPlan = BottomUp[LogicalOperator](LogicalOptimizer.replaceCartesianWithValueJoin).transform(filter)

      val flippedEquals = Equals(propA, propB)(CTBoolean)
      val projectA = Project(propA -> None, scanA, scanA.solved)
      val projectB = Project(propB -> None, scanB, scanB.solved)
      val solved = SolvedQueryModel(Set(irFieldA, irFieldB)).withPredicate(flippedEquals)
      val valueJoin = ValueJoin(projectA, projectB, Set(flippedEquals), solved)

      optimizedPlan should equalWithTracing(valueJoin)
    }
  }

  private def logicalPlan(query: String, schema: Schema): LogicalOperator = {
    val producer = new LogicalOperatorProducer
    val logicalPlanner = new LogicalPlanner(producer)
    val ir = query.asCypherQuery(testGraphName -> schema)(schema)
    val logicalPlannerContext = plannerContext(schema)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logicalPlan
  }

}
