package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.common.TreeNode
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator

class PhysicalOptimizer
  extends DirectCompilationStage[PhysicalOperator, PhysicalOperator, RuntimeContext] {

  override def process(input: PhysicalOperator)(implicit context: RuntimeContext): PhysicalOperator = {
    val opCounts = identifyDuplicates(input)
    opCounts.keys.foldLeft(input) {
      case (rewritten, current) => insertCacheOps(current)(rewritten)
    }

    // build replacement map
  }

  def insertCacheOp(replacements: Map[PhysicalOperator, PhysicalOperator]): TreeNode.RewriteRule[PhysicalOperator] =
    TreeNode.RewriteRule {
      case op if replacements.contains(op) => replacements(op)
    }

  def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
    input.foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
      case (agg, op) => agg.updated(op, agg(op) + 1)
    }.filter(_._2 > 1)
  }

  def insertCacheOps(target: PhysicalOperator)(op: PhysicalOperator): PhysicalOperator = {
    ???
  }
}
