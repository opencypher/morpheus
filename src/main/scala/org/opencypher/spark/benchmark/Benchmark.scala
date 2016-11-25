package org.opencypher.spark.benchmark


trait Benchmark[G] {
  def name: String
  def run(graph: G): Outcome
}

case class BenchmarkResult(name: String, times: Seq[Long], plan: String, count: Long, checksum: Int) {
  lazy val min = times.min
  lazy val max = times.max
  lazy val avg = times.sum / times.length
  lazy val median = times.sorted.apply(times.length / 2)

  override def toString: String =
    s"$name: \t$avg [$min .. $median .. $max] ms\t${avg.toDouble/count * 1000} avg ms/Krow\t$count rows\t$checksum checksum"
}

case class BenchmarkSummary(query: String, nodes: Long, relationships: Long) {
  override def toString: String = s"Running query $query\non a graph with $nodes nodes and $relationships relationships."
}

trait Outcome {
  def computeCount: Long
  def computeChecksum: Int
  def plan: String
}

