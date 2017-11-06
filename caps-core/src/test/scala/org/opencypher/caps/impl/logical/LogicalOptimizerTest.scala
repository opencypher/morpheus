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

import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.impl.IrTestSuite

import scala.language.implicitConversions

class LogicalOptimizerTest extends IrTestSuite {

  val nodeA = IRField("a")(CTNode)
  val nodeB = IRField("b")(CTNode)
  val nodeG = IRField("g")(CTNode)
  val relR = IRField("r")(CTRelationship)

  val producer = new LogicalOperatorProducer

  def plannerContext(schema: Schema) = LogicalPlannerContext(schema, Set.empty, (_) => testGraphSource)

  test("rewrite select void") {
    val query = """MATCH (a:Animal)
    RETURN a.name"""

    val schema = Schema.empty
    val plan = logicalPlan(query, schema)
    println(plan)
    val logicalOptimizer = new LogicalOptimizer(producer)
    val optimizedLogicalPlan = logicalOptimizer(plan)(plannerContext(schema))
    println(optimizedLogicalPlan)
    //optimizedLogicalPlan should equal()
  }

  test("rewrite missing label combination") {
    val query = """MATCH (a:Animal)
    RETURN a"""

    val schema = Schema.empty
    val plan = logicalPlan(query, schema)
    println(plan)
    val logicalOptimizer = new LogicalOptimizer(producer)
    val optimizedLogicalPlan = logicalOptimizer(plan)(plannerContext(schema))
    println(optimizedLogicalPlan)
    //optimizedLogicalPlan should equal()
  }

  private def logicalPlan(query: String, schema: Schema): LogicalOperator = {
    val logicalPlanner = new LogicalPlanner(producer)
    val ir = query.ir
    val logicalPlannerContext = plannerContext(schema)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logicalPlan
  }
}
