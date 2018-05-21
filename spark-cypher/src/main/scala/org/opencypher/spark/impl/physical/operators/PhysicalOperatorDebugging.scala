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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.configuration.CAPSConfiguration.DebugPhysicalOperators
import org.opencypher.spark.impl.physical.operators.PhysicalOperatorDebugging.separator
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.util.Profiling.printTiming
import org.opencypher.spark.impl.{CAPSPatternGraph, CAPSRecords, CAPSUnionGraph}
import org.opencypher.spark.impl.util.SparkQueryPlanCostEstimation._

trait PhysicalOperatorDebugging extends CAPSPhysicalOperator {

  abstract override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    if (DebugPhysicalOperators.get) {
      val operatorName = getClass.getSimpleName.toUpperCase
      val output: CAPSPhysicalResult = super.execute
      implicit val caps: CAPSSession = output.records.caps
      println
      println(separator)
      println(s"**$operatorName**")

      val cachedRecordsDf: DataFrame = printExecutionTimingAndPlanThenCacheAndForce("Compute result records", output.records.data)
      val cachedRecords = CAPSRecords.verifyAndCreate(output.records.header -> cachedRecordsDf)

      if (getClass == classOf[ConstructGraph]) {
        output.workingGraph match {
          case ug: CAPSUnionGraph =>
            ug.graphs.collectFirst {
              case (patternGraph: CAPSPatternGraph, _) => patternGraph
            }.map { pg =>
              printExecutionTimingAndPlanThenCacheAndForce("Compute pattern graph", pg.baseTable.data)
            }
        }
      }

      println(separator)
      println

      val cachedResult = output.copy(records = cachedRecords)
      cachedResult
    } else {
      super.execute
    }
  }

  /**
    * Prints computation timing and Spark plan for a DF, then returns a cached and forced version of that DF.
    */
  def printExecutionTimingAndPlanThenCacheAndForce(description: String, df: DataFrame): DataFrame = {
    // Print computation timing
    printTiming(s"$description") {
      df.count() // Force evaluation of operator
    }

    // Print Spark plan
    println("Spark plan:")
    val sparkPlan = df.queryExecution.optimizedPlan
    implicit val sparkSession = df.sparkSession
    // Remove cached inputs from plan
    val planWithoutCached = sparkPlan.transformDown {
      case imr: InMemoryRelation => CachedOperatorInput
      case other => other
    }
    println(planWithoutCached.treeString)

    // Cache
    val cachedDf = df.cache()

    // Force
    cachedDf.count() // Force evaluation of cached DF
    cachedDf
  }

}

object PhysicalOperatorDebugging {

  val separator = "=" * 80

}

case object CachedOperatorInput extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty
}
