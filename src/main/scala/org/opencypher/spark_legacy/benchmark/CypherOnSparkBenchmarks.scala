/**
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
package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark_legacy.impl.{StdPropertyGraph, SupportedQuery}

object CypherOnSparkBenchmarks extends SupportedQueryBenchmarks[StdPropertyGraph] {

  override def apply(query: SupportedQuery): Benchmark[StdPropertyGraph] = new Benchmark[StdPropertyGraph] {

    override def name: String = "CoS       "

    override def numNodes(graph: StdPropertyGraph): Long = graph.nodes.count()
    override def numRelationships(graph: StdPropertyGraph): Long = graph.relationships.count()

    override def plan(graph: StdPropertyGraph) =
      graph.cypher(query).products.toDS.queryExecution.toString()

    override def run(graph: StdPropertyGraph): Outcome = {
      val result = graph.cypher(query).products.toDS

      val (count, checksum) = result.rdd.treeAggregate((0, 0))({
        case ((c, cs), product) => (c + 1, cs ^ product.productElement(0).hashCode())
      }, {
        case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
      })

      new Outcome {
        override def computeCount: Long = count
        override def computeChecksum: Int = checksum
        override def usedCachedPlan: Boolean = false
      }
    }
  }
}
