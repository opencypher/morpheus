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
package org.opencypher.okapi.relational.impl.planning

import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators.{Cache, RelationalOperator, Start}
import org.opencypher.okapi.trees.TopDown

object RelationalOptimizer {

  def process[T <: Table[T]](input: RelationalOperator[T]): RelationalOperator[T] = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators {

    def apply[T <: Table[T]](input: RelationalOperator[T]): RelationalOperator[T] = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start[T] => false
        case _ => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[RelationalOperator[T]] {
        case cache: Cache[T] => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.transform(input)
    }

    private def calculateReplacementMap[T <: Table[T]](input: RelationalOperator[T]): Map[RelationalOperator[T], RelationalOperator[T]] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[RelationalOperator[T]] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache, currentCounts) =>
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
      opsToCache.map(op => op -> Cache[T](op)).toMap
    }

    private def identifyDuplicates[T <: Table[T]](input: RelationalOperator[T]): Map[RelationalOperator[T], Int] = {
      input
        .foldLeft(Map.empty[RelationalOperator[T], Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
