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

import org.opencypher.caps.api.graph.{CypherResultPlan, Plan}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult}
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator
import org.opencypher.caps.logical.impl.LogicalOperator

object CAPSResultBuilder {
  def from(logical: LogicalOperator, flat: FlatOperator, physical: PhysicalOperator)(
      implicit context: RuntimeContext): CAPSResult = {
    new CAPSResult {
      lazy val result: PhysicalResult = physical.execute

      override def records: CAPSRecords = result.records

      override def graphs: Map[String, CAPSGraph] = result.graphs

      override def explain: Plan[LogicalOperator, FlatOperator, PhysicalOperator] = {
        Plan(CypherResultPlan(logical), CypherResultPlan(flat), CypherResultPlan(physical))
      }
    }
  }
}
