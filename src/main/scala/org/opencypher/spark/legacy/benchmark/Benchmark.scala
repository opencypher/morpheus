package org.opencypher.spark.legacy.benchmark

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
