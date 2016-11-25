package org.opencypher.spark.benchmark

object BenchmarkSeries {
  def run[B <: Benchmark[G], G](benchmarkAndGraph: (Benchmark[G], G), nbrTimes: Int = 10): BenchmarkResult = {
    val (benchmark, graph) = benchmarkAndGraph
    val outcome = benchmark.run(graph)
    val plan = outcome.plan
    val count = outcome.computeCount
    val checksum = outcome.computeChecksum

    val outcomes = (0 until nbrTimes).map { i =>
      println(s"Timing -- Run $i")
      val start = System.currentTimeMillis()
      val outcome = benchmark.run(graph)
      val time = System.currentTimeMillis() - start
      println(s"Done -- $time ms")
      time -> outcome
    }
    BenchmarkResult(benchmark.name, outcomes.map(_._1), plan, count, checksum)
  }
}

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
    s"$name: $avg [$min .. $median .. $max] ms\t${avg.toDouble/count} avg ms/row\t$count rows\t$checksum checksum"
}

case class BenchmarkSummary(query: String, nodes: Long, relationships: Long) {
  override def toString: String = s"Running query $query\non a graph with $nodes nodes and $relationships relationships."
}

trait Outcome {
  def computeCount: Long
  def computeChecksum: Int
  def plan: String
}

