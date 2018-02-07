/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.caps.impl.spark.physical.operators.{Cache, CAPSPhysicalOperator, Start, StartFromUnit}
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.trees.TopDown

case class PhysicalOptimizerContext()

class PhysicalOptimizer extends DirectCompilationStage[CAPSPhysicalOperator, CAPSPhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: CAPSPhysicalOperator)(implicit context: PhysicalOptimizerContext): CAPSPhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (CAPSPhysicalOperator => CAPSPhysicalOperator) {
    def apply(input: CAPSPhysicalOperator): CAPSPhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start | _: StartFromUnit => false
        case _                           => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[CAPSPhysicalOperator] {
        case cache: Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.rewrite(input)
    }

    private def calculateReplacementMap(input: CAPSPhysicalOperator): Map[CAPSPhysicalOperator, CAPSPhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[CAPSPhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache: Set[CAPSPhysicalOperator], currentCounts: Map[CAPSPhysicalOperator, Int]) =>
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

    private def identifyDuplicates(input: CAPSPhysicalOperator): Map[CAPSPhysicalOperator, Int] = {
      input
        .foldLeft(Map.empty[CAPSPhysicalOperator, Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
