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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.api.graph.CypherQueryPlans
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.trees.TreeNode
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSResult}

object CAPSResultBuilder {

  def from(logical: LogicalOperator, flat: FlatOperator, physical: CAPSPhysicalOperator): CAPSResult = {

    implicit def session: CAPSSession = physical.context.session

    new CAPSResult {
      lazy val result: CAPSPhysicalResult = CAPSPhysicalResult(
        CAPSRecords(physical.header, physical.table.df, physical.returnItems.map(_.map(_.withoutType))),
        physical.graph,
        physical.graphName,
        physical.tagStrategy
      )

      override def records: Option[CAPSRecords] = Some(result.records)

      override def graph: Option[CAPSGraph] = Some(result.workingGraph)

      override def plans = CAPSQueryPlans(Some(logical), Some(flat), Some(physical))

    }
  }
}

case class CAPSQueryPlans(
  logicalPlan: Option[TreeNode[LogicalOperator]],
  flatPlan: Option[TreeNode[FlatOperator]],
  physicalPlan: Option[TreeNode[CAPSPhysicalOperator]]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.map(_.pretty).getOrElse("")

  override def physical: String = physicalPlan.map(_.pretty).getOrElse("")
}

object CAPSQueryPlans {
  def empty: CAPSQueryPlans = CAPSQueryPlans(None, None, None)
}
