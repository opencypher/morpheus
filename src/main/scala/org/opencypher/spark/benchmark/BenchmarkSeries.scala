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
