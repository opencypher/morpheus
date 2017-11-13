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
package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.api.graph.CypherResultPlan
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult}
import org.opencypher.caps.impl.common.Tree
import org.opencypher.caps.impl.logical.LogicalOperator

object CAPSResultBuilder {
  def from(physical: Tree[PhysicalOperator], plan: LogicalOperator)(implicit context: RuntimeContext): CAPSResult =
    new CAPSResult {
      val execute = Execute()
      lazy val result: PhysicalResult = execute(physical)

      override def records: CAPSRecords = result.records
      override def graphs: Map[String, CAPSGraph] = result.graphs

      override def explain: CypherResultPlan = CypherResultPlan(plan)
    }
}

case class Execute()(implicit context: RuntimeContext) extends Tree.Aggregate[PhysicalOperator, PhysicalResult] {
  override def apply(operator: PhysicalOperator, inputs: Seq[PhysicalResult]) = {
    operator.execute(inputs: _*)
  }
}
