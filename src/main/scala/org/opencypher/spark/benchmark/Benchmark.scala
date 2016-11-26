package org.opencypher.spark.benchmark

case class BenchmarkAndGraph[G](benchmark: Benchmark[G], graph: G) {
  def use[T](f: (Benchmark[G], G) => T) = f(benchmark, graph)
}

trait Benchmark[G] {
  self =>

  def name: String
  def run(graph: G): Outcome

  def using(graph: G) = BenchmarkAndGraph(self, graph)

  def numNodes(graph: G): Long
  def numRelationships(graph: G): Long
}

case class BenchmarkResult(name: String, times: Seq[Long], plan: String, count: Long, checksum: Int) {
  lazy val min = times.min
  lazy val max = times.max
  lazy val avg = times.sum / times.length
  lazy val median = times.sorted.apply(times.length / 2)

  override def toString: String =
    s"$name: \t$avg [$min .. $median .. $max] ms\t${count.toDouble/avg.toDouble} avg Krow/sec\t$count rows\t$checksum checksum"
}

case class BenchmarkSummary(query: String, nodes: Long, relationships: Long) {
  override def toString: String = s"Benchmarking query $query\nbased on a source graph with $nodes nodes and $relationships relationships."
}

trait Outcome {
  def computeCount: Long
  def computeChecksum: Int
  def plan: String
}
