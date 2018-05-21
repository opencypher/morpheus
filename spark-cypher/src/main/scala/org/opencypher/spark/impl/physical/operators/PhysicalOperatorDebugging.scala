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
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.configuration.CAPSConfiguration.DebugPhysicalOperators
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.operators.PhysicalOperatorDebugging.separator
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSGraph, CAPSPatternGraph, CAPSRecords, CAPSUnionGraph}

trait PhysicalOperatorDebugging extends CAPSPhysicalOperator {

  abstract override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    if (DebugPhysicalOperators.get) {
      val output: CAPSPhysicalResult = super.execute
      implicit val caps: CAPSSession = output.records.caps

      val operatorName = getClass.getSimpleName.toUpperCase
      println
      println(separator)
      println(s"**$operatorName**")

      val recordsDf = output.records.data
      println
      recordsDf.printExecutionTiming(s"Computing $operatorName result records")
      println
      recordsDf.printLogicalPlan
      recordsDf.cacheAndForce(Some(operatorName))

      if (getClass == classOf[ConstructGraph]) {
        output.workingGraph match {
          case unionGraph: CAPSUnionGraph =>
            unionGraph.graphs.collectFirst {
              case (patternGraph: CAPSPatternGraph, retaggings) =>
                val baseTableDf = patternGraph.baseTable.data
                baseTableDf.printExecutionTiming("Computing pattern graph")
                println
                baseTableDf.printLogicalPlan
                baseTableDf.cacheAndForce(Some("CAPSPatternGraph"))
            }
        }
      }

      val inputs = maybeChildResults.getOrElse(
        throw UnsupportedOperationException(s"Operator $operatorName did not store its input results"))

      if (inputs.nonEmpty) {
        println("Input operators:")
        inputs.foreach { case (operator, result) =>
          println(s"\t${operator.getClass.getSimpleName.toUpperCase} with ${result.records.size} records")
        }
        println
      }

      println(separator)
      println

      output
    } else {
      super.execute
    }
  }

}

object PhysicalOperatorDebugging {

  val separator = "=" * 80

}

case class CachedOperatorInput(tableName: Option[String] = None) extends LeafNode {

  override def output: Seq[Attribute] = Seq.empty

}
