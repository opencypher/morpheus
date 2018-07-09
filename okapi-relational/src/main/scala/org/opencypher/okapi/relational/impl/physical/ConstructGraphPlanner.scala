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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.logical.impl.{LogicalOperator, LogicalPatternGraph}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.{RelationalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators.RelationalOperator

object ConstructGraphPlanner {

  def planConstructGraph[T <: FlatRelationalTable[T]](in: Option[LogicalOperator], construct: LogicalPatternGraph)
    (implicit context: RelationalPlannerContext[T]): RelationalOperator[T] = {

    import planner.producer._

    val onGraphPlan = {
      construct.onGraphs match {
        case Nil => planStart() // Empty start
        //TODO: Optimize case where no union is necessary
        //case h :: Nil => operatorProducer.planStart(Some(h)) // Just one graph, no union required
        case several =>
          val onGraphPlans = several.map(qgn => planStart(Some(qgn)))
          planGraphUnionAll(onGraphPlans, construct.name)
      }
    }
    val inputTablePlan = in.map(planner.process).getOrElse(planStart())

    val constructGraphPlan = planner.producer.planConstructGraph(inputTablePlan, onGraphPlan, construct)
    context.constructedGraphPlans.update(construct.name, constructGraphPlan)
    constructGraphPlan
  }
}
