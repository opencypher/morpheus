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

import scala.math.BigDecimal.RoundingMode.HALF_UP

case class BenchmarkAndGraph[G](benchmark: Benchmark[G], graph: G) {
  def use[T](f: (Benchmark[G], G) => T) = f(benchmark, graph)
}

trait Benchmark[G] {
  self =>

  def name: String

  def init(graph: G): Unit = ()

  def plan(graph: G): String
  def run(graph: G): Outcome

  def using(graph: G) = BenchmarkAndGraph(self, graph)

  def numNodes(graph: G): Long
  def numRelationships(graph: G): Long
}

case class BenchmarkResult(name: String, times: Seq[Long], plan: String, count: Long, checksum: Int) {
  lazy val min = times.min
  lazy val max = times.max
  lazy val avg = BigDecimal(times.sum.toDouble / times.length).setScale(2, HALF_UP)
  lazy val median = times.sorted.apply(times.length / 2)

  def summary(slowest: Option[BigDecimal]): String = {
    val normalized = slowest.map(bd => (bd / avg).setScale(2, HALF_UP).toString + "x").getOrElse("")
    s"$name: \t$avg $normalized [$min .. $median .. $max] ms\t${count.toDouble/avg.toDouble} avg Krow/sec\t$count rows\t$checksum checksum"
  }

  override def toString: String = summary(None)
}

case class BenchmarkSummary(query: String, nodes: Long, relationships: Long) {
  override def toString: String = s"Benchmarking query $query\nbased on a source graph with $nodes nodes and $relationships relationships."
}

trait Outcome {
  def computeCount: Long
  def computeChecksum: Int
  def usedCachedPlan: Boolean
}
