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
package org.opencypher.spark.impl.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation

object SparkQueryPlanCostEstimation {

  implicit class RichLogicalSparkPlan(val sparkPlan: LogicalPlan) extends AnyVal {

    /**
      * Returns the estimated cost of the `sparkPlan` root operator.
      */
    def operatorCost(implicit session: SparkSession): Long = {
      sparkPlan match {
        // Estimated cost for InMemoryRelation is Long.MaxValue, which is unreasonable.
        // Estimate instead as cost of its direct children.
        case m: InMemoryRelation => m.children.map(_.operatorCost).sum
        case node =>
          val estimate = sparkPlan.stats(session.sessionState.conf).sizeInBytes.toLong
          if (estimate > 0L) estimate else 0L
      }
    }

    /**
      * Returns the estimated cost of `sparkPlan`.
      */
    def cost(implicit session: SparkSession): Long = {
      sparkPlan.map(identity).distinct.map(_.operatorCost).sum
    }

    /**
      * Returns all distinct operators in the `sparkPlan` operator tree with their estimated cost.
      */
    def explainCost(implicit session: SparkSession): Seq[(String, Long)] = {
      val distinctTreeNodes = sparkPlan.map(identity).distinct
      distinctTreeNodes.map { n =>
        n.getClass.getSimpleName -> n.operatorCost
      }
    }

  }

}
