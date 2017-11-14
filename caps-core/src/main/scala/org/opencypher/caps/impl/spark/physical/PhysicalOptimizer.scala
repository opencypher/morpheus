package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.common.TreeNode
import org.opencypher.caps.impl.spark.physical.operators.{Cache, PhysicalOperator}

class PhysicalOptimizer
  extends DirectCompilationStage[PhysicalOperator, PhysicalOperator, RuntimeContext] {

  override def process(input: PhysicalOperator)(implicit context: RuntimeContext): PhysicalOperator = {
    val opCounts: Map[PhysicalOperator, Int] = identifyDuplicates(input)
    val opsByHeight = opCounts.keys.toSeq.sortBy(-_.height)
    val (opsToCache, _) = opsByHeight.foldLeft((Set.empty[PhysicalOperator], opCounts)) {
      case ((currentOpsToCache: Set[PhysicalOperator], currentCounts: Map[PhysicalOperator, Int]), currentOp: PhysicalOperator) =>
        val currentOpCount = currentCounts(currentOp)
        if (currentOpCount > 1) {
          (currentOpsToCache + currentOp, currentCounts.map { case (op, count) =>
            (op, if (currentOp.contains(op)) count - 1 else count)
          })
        } else {
          (currentOpsToCache, currentCounts)
        }
    }

    val cacheMap: Map[PhysicalOperator, PhysicalOperator] = opsToCache.map(op => op -> Cache(op)).toMap

    input.transformUp(insertCacheOps(cacheMap))
  }

  def insertCacheOps(replacements: Map[PhysicalOperator, PhysicalOperator]): TreeNode.RewriteRule[PhysicalOperator] =
    TreeNode.RewriteRule {
      case op if replacements.contains(op) => replacements(op)
    }

  def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
    input.foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
      case (agg, op) => agg.updated(op, agg(op) + 1)
    }.filter(_._2 > 1)
  }

}
