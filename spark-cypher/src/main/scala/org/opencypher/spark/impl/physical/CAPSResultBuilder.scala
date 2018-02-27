/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.api.graph.CypherQueryPlans
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.trees.TreeNode
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSResult}

object CAPSResultBuilder {

  def from(logical: LogicalOperator, flat: FlatOperator, physical: CAPSPhysicalOperator)(
    implicit context: CAPSRuntimeContext): CAPSResult = {

    new CAPSResult {
      lazy val result: CAPSPhysicalResult = physical.execute

      override def records: CAPSRecords = result.records

      override def graphs: Map[String, CAPSGraph] = result.graphs

      override def plans = CAPSQueryPlans(logical, flat, physical)

    }
  }
}

case class CAPSQueryPlans(
  logicalPlan: TreeNode[LogicalOperator],
  flatPlan: TreeNode[FlatOperator],
  physicalPlan: TreeNode[CAPSPhysicalOperator]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.pretty

  override def physical: String = physicalPlan.pretty

}
