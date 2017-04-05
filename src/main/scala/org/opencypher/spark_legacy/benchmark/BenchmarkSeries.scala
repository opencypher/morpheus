package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark_legacy.benchmark.Configuration.{Runs, WarmUpRuns}

object BenchmarkSeries {
  def run[G](benchmarkAndGraph: BenchmarkAndGraph[G], runs: Int = Runs.get(), warmupRuns: Int = WarmUpRuns.get()): BenchmarkResult = {
    val (planTime, plan, count, checksum) = warmup(benchmarkAndGraph, warmupRuns)
    measure(benchmarkAndGraph, runs, planTime, plan, count, checksum)
  }

  private def warmup[G](benchmarkAndGraph: BenchmarkAndGraph[G], warmUpRuns: Int): (Long, String, Long, Int) = {
    benchmarkAndGraph.use { (benchmark, graph) =>
      benchmark.init(graph)
      val name = benchmark.name

      val (planTime, plan, count, checksum) = wrap(s"Initial run for $name") {
        val (planTime, plan) = planAndTime(name)(benchmark.plan(graph))
        val (_, outcome) = runAndTime(0, benchmark.run(graph))
        wrap(s"Plan for $name")(println(plan))
        val count = outcome.computeCount
        println(s"Count -- $count")
        val checksum = outcome.computeChecksum
        println(s"Checksum -- $checksum")
        (planTime, plan, count, checksum)
      }

      wrap(s"Warmup for $name") {
        (1 until warmUpRuns).foreach { i =>
          runAndTime(i, benchmark.run(graph))
        }
      }

      (planTime, plan, count, checksum)
    }
  }

  private def measure[G](benchmarkAndGraph: BenchmarkAndGraph[G], runs: Int, planTime: Long, plan: String, count: Long, checksum: Int) = {
    benchmarkAndGraph.use { (benchmark, graph) =>
      wrap(s"Measurements for ${benchmark.name}") {
        val outcomes = (0 until runs).map { i =>
          val (time, outcome) = runAndTime(i, benchmark.run(graph))
          if (outcome.usedCachedPlan) time + planTime else time
        }
        BenchmarkResult(benchmark.name, outcomes, plan, count, checksum)
      }
    }
  }

  private def planAndTime(name: String)(f: => String): (Long, String) = {
    println(s"Timing -- Plan")
    val start = System.currentTimeMillis()
    val plan = f
    val time = System.currentTimeMillis() - start
    println(s"Done -- $time ms")
    time -> plan
  }

  private def runAndTime(i: Int, f: => Outcome): (Long, Outcome) = {
    println(s"Timing -- Run $i")
    val start = System.currentTimeMillis()
    val outcome = f
    val time = System.currentTimeMillis() - start
    println(s"Done -- $time ms")
    time -> outcome
  }

  private def wrap[T](header: String)(f: => T): T = {
    println(s">>>>> $header")
    val result = f
    println(s"<<<<< $header")
    result
  }
}
