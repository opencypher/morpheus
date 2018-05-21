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
package org.opencypher.spark.impl.physical.operators

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.configuration.CAPSConfiguration.DebugPhysicalOperators
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.operators.PhysicalOperatorDebugging.separator
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSGraph, CAPSPatternGraph, CAPSRecords, CAPSUnionGraph}

trait PhysicalOperatorDebugging extends CAPSPhysicalOperator {

  abstract override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    if (DebugPhysicalOperators.get) {
      val operatorName = getClass.getSimpleName.toUpperCase
      val output: CAPSPhysicalResult = super.execute
      implicit val caps: CAPSSession = output.records.caps
      println
      println(separator)
      println(s"**$operatorName**")

      val recordsDf = output.records.data
      val cachedRecordsDf = {
        recordsDf.printExecutionTiming(s"Computing $operatorName result records")
        recordsDf.printLogicalPlan
        recordsDf.cacheAndForce
      }
      val cachedRecords = CAPSRecords.verifyAndCreate(output.records.header -> cachedRecordsDf)

      val maybeCachedGraph: Option[CAPSGraph] = if (getClass != classOf[ConstructGraph]) {
        None
      } else {
        output.workingGraph match {
          case unionGraph: CAPSUnionGraph =>
            unionGraph.graphs.collectFirst {
              case (patternGraph: CAPSPatternGraph, retaggings) =>
              val baseTableDf = patternGraph.baseTable.data
              baseTableDf.printExecutionTiming("Computing pattern graph")
              baseTableDf.printLogicalPlan
              val cachedBaseTableDf = baseTableDf.cacheAndForce
              val cachedBaseTable = CAPSRecords.verifyAndCreate(patternGraph.baseTable.header, cachedBaseTableDf)
              val cachedPatternGraph = patternGraph.copy(baseTable = cachedBaseTable)
              val unionGraphWithCachedPatternGraph = unionGraph.copy(
                graphs = (unionGraph.graphs - patternGraph) + (cachedPatternGraph -> retaggings))
              unionGraphWithCachedPatternGraph
            }
        }
      }

      println(separator)
      println

      val cachedResult = output.copy(
        records = cachedRecords,
        workingGraph = maybeCachedGraph.getOrElse(output.workingGraph)
      )
      cachedResult
    } else {
      super.execute
    }
  }

}

object PhysicalOperatorDebugging {

  val separator = "=" * 80

}

case object CachedOperatorInput extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty
}
