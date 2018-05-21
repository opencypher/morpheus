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
    // Remove cached inputs from plan
    val planWithoutCached = sparkPlan.transformDown {
      case _: InMemoryRelation => CachedOperatorInput()
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

case class CachedOperatorInput() extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty
}
