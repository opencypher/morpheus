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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.configuration.CAPSConfiguration.DebugPhysicalOperators
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.operators.PhysicalOperatorDebugging.separator
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSPatternGraph, CAPSUnionGraph}

trait PhysicalOperatorDebugging extends CAPSPhysicalOperator {

  /**
    * Wrapper around execute that measures how long an operator takes to execute.
    *
    * If all input records are run with it as well, then they are cached and their computation was forced before measuring
    * how long forcing the computation required by the current operator takes.
    *
    * Debugging computes every operator twice: once to measure how long the computation takes and once to force the
    * computation of a cached version of its result. The reason for this is that the caching itself might be associated
    * with overhead that we would like to exclude from the measurement.
    *
    * @param context backend-specific runtime context
    * @return physical result
    */
  abstract override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    if (DebugPhysicalOperators.get) {
      val output: CAPSPhysicalResult = super.execute
      implicit val caps: CAPSSession = output.records.caps

      val operatorName = toString
      val simpleOperatorName = getClass.getSimpleName

      println
      println(separator)
      println(s"**$operatorName**")

      val recordsDf = output.records.data
      if (output.records.header != RecordHeader.empty) {
        println
        recordsDf.printExecutionTiming(s"Computing $simpleOperatorName output records DataFrame")
        println
        recordsDf.printPhysicalPlan
      }
      val outputRecordsDfRowCount = recordsDf.cacheAndForce(Some(operatorName))

      if (getClass == classOf[ConstructGraph]) {
        output.workingGraph match {
          case unionGraph: CAPSUnionGraph =>
            unionGraph.graphs.collectFirst {
              case (patternGraph: CAPSPatternGraph, retaggings) =>
                val baseTableDf = patternGraph.baseTable.data
                baseTableDf.printExecutionTiming("Computing pattern graph")
                println
                baseTableDf.printPhysicalPlan
                val baseTableRowCount = baseTableDf.cacheAndForce(Some(operatorName))
                println(s"Pattern graph base table DataFrame has $baseTableRowCount rows")
            }
        }
      }

      val inputs = maybeChildResults.getOrElse(
        throw UnsupportedOperationException(s"Operator $simpleOperatorName did not store its input results"))

      if (inputs.nonEmpty) {
        println("Inputs:")
        inputs.foreach { case (operator, result) =>
          println(s"\t${operator.getClass.getSimpleName.toUpperCase} records DataFrame has $outputRecordsDfRowCount rows")
        }
        println
      }
      println(s"Output records DataFrame has $outputRecordsDfRowCount rows")
      println

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

/**
  * Placeholder operator to allow for more readable Spark plan trees that can name tables according to the CAPS operator
  * that produces them.
  *
  * @param tableName name of the table, in CAPS used to name the table after the operator that produced it
  */
case class CachedOperatorInput(tableName: Option[String] = None)(implicit sc: SparkContext) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = sc.emptyRDD

  override def output: Seq[Attribute] = Seq.empty

}
