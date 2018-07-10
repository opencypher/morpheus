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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators.{Cache, RelationalOperator, Start}
import org.opencypher.okapi.trees.TopDown

case class RelationalOptimizerContext()

class RelationalOptimizer[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RelationalRuntimeContext[O, K, A, P, I]] extends DirectCompilationStage[K, K, RelationalOptimizerContext] {

  override def process(input: K)(implicit context: RelationalOptimizerContext): K = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (K => K) {
    def apply(input: K): K = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start[O, K, A, P, I] => false
        case _                           => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[RelationalOperator[O, K, A, P, I]] {
        case cache: Cache[O, K, A, P, I] => cache
          // TODO: fix self type issue
        case parent if (parent.childrenAsSet.asInstanceOf[Set[K]] intersect nodesToReplace).nonEmpty =>
          val newChildren: Array[RelationalOperator[O, K, A, P, I]] = parent.children.asInstanceOf[Array[K]].map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren).asInstanceOf[K]
      }.rewrite(input).asInstanceOf[K]
    }

    private def calculateReplacementMap(input: K): Map[K, K] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[K] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache: Set[K], currentCounts: Map[K, Int]) =>
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
      // TODO: filter by opCount and always point at first op in group (avoids mutable map)
      opsToCache.map(op => op -> Cache[O, K, A, P, I](op).asInstanceOf[K]).toMap
    }

    private def identifyDuplicates(input: K): Map[K, Int] = {
      input
        .foldLeft(Map.empty[K, Int].withDefaultValue(0)) {
          // TODO: fix self type issue
          case (agg, op) => agg.updated(op.asInstanceOf[K], agg(op.asInstanceOf[K]) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
