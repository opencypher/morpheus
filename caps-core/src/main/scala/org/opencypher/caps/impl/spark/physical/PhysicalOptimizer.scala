/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.common.TreeNode
import org.opencypher.caps.impl.spark.physical.operators.{Cache, PhysicalOperator, Start, StartFromUnit}

case class PhysicalOptimizerContext()

class PhysicalOptimizer
  extends DirectCompilationStage[PhysicalOperator, PhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: PhysicalOperator)(implicit context: PhysicalOptimizerContext): PhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (PhysicalOperator => PhysicalOperator) {
    def apply(input: PhysicalOperator): PhysicalOperator = {
      val replacements = calculateReplacementMap(input)
        .filterKeys {
          case _:Start | _:StartFromUnit => false
          case _ => true
        }

      val nodesToReplace = replacements.keySet
      val rule = TreeNode.RewriteRule[PhysicalOperator] {
        case cache : Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
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
