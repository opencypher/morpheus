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

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, _}
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.impl.IrTestSuite

import scala.language.implicitConversions

class LogicalOptimizerTest extends IrTestSuite {

  val producer = new LogicalOperatorProducer
  val emptySqm = SolvedQueryModel.empty[Expr]
  val logicalGraph = LogicalExternalGraph(testGraph.name, null, Schema.empty)
  val schema = Schema.empty

//  //Helper to create nicer expected results with `asCode`
//  implicit val specialMappings = Map[Any, String](
//    schema -> "schema",
//    emptySqm -> "emptySqm",
//    logicalGraph -> "logicalGraph",
//    emptySqm -> "emptySqm",
//    (CTNode: CTNode) -> "CTNode"
//  )

  def plannerContext(schema: Schema) = LogicalPlannerContext(schema, Set.empty, (_) => testGraphSource)

  test("rewrite missing label scan to empty records") {
    val query = """
                  | MATCH (a:Animal)
                  | RETURN a""".stripMargin
    val plan = logicalPlan(query, schema)
    val logicalOptimizer = new LogicalOptimizer(producer)
    val optimizedLogicalPlan = logicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      Vector(Var("a", CTNode(Set("Animal")))),
      Set(),
      EmptyRecords(
        Set(Var("a", CTNode(Set("Animal")))),
        SetSourceGraph(logicalGraph, Start(logicalGraph, Set(), emptySqm), emptySqm),
        SolvedQueryModel(Set(), Set(HasLabel(Var("a", CTNode(Set("Animal"))), Label("Animal"), CTBoolean)), Set())
      ),
      SolvedQueryModel(Set(IRField("a", CTNode)), Set(HasLabel(Var("a", CTNode), Label("Animal"), CTBoolean)), Set())
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  test("rewrite missing label combination") {
    val query = """
                  | MATCH (a:Animal:Astronaut)
                  | RETURN a""".stripMargin
    val schema = Schema.empty.withNodePropertyKeys("Animal")().withNodePropertyKeys("Astronaut")()
    val plan = logicalPlan(query, schema)
    val logicalOptimizer = new LogicalOptimizer(producer)
    val optimizedLogicalPlan = logicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      Vector(Var("a", CTNode(Set("Animal", "Astronaut")))),
      Set(),
      EmptyRecords(
        Set(Var("a", CTNode(Set("Astronaut", "Animal")))),
        SetSourceGraph(logicalGraph, Start(logicalGraph, Set(), emptySqm), emptySqm),
        SolvedQueryModel(
          Set(),
          Set(
            HasLabel(Var("a", CTNode(Set("Astronaut", "Animal"))), Label("Astronaut"), CTBoolean),
            HasLabel(Var("a", CTNode(Set("Astronaut", "Animal"))), Label("Animal"), CTBoolean)
          ),
          Set()
        )
      ),
      SolvedQueryModel(
        Set(IRField("a", CTNode)),
        Set(
          HasLabel(Var("a", CTNode), Label("Animal"), CTBoolean),
          HasLabel(Var("a", CTNode), Label("Astronaut"), CTBoolean)),
        Set())
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  private def logicalPlan(query: String, schema: Schema): LogicalOperator = {
    val logicalPlanner = new LogicalPlanner(producer)
    val ir = query.ir
    val logicalPlannerContext = plannerContext(schema)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logicalPlan
  }

}
