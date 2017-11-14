package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.common.TreeNode
import org.opencypher.caps.impl.spark.physical.operators.{Cache, PhysicalOperator}

case class PhysicalOptimizerContext()

class PhysicalOptimizer
  extends DirectCompilationStage[PhysicalOperator, PhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: PhysicalOperator)(implicit context: PhysicalOptimizerContext): PhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (PhysicalOperator => PhysicalOperator) {
    def apply(input: PhysicalOperator): PhysicalOperator = {
      val replacements = calculateReplacementMap(input)

      val rule = TreeNode.RewriteRule[PhysicalOperator] {
        case cache : Cache => cache
        case parent if (parent.children.toSet intersect replacements.keySet).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }

      input.transformDown(rule)
    }

    private def calculateReplacementMap(input: PhysicalOperator): Map[PhysicalOperator, PhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a,b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[PhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache: Set[PhysicalOperator], currentCounts: Map[PhysicalOperator, Int]) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - 1 else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }

      opsToCache.map(op => op -> Cache(op)).toMap
    }

    private def identifyDuplicates(input: PhysicalOperator): Map[PhysicalOperator, Int] = {
      input.foldLeft(Map.empty[PhysicalOperator, Int].withDefaultValue(0)) {
        case (agg, op) => agg.updated(op, agg(op) + 1)
      }.filter(_._2 > 1)
    }
  }
}

