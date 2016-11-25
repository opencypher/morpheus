package org.opencypher.spark.benchmark

import org.opencypher.spark.impl.SupportedQuery

trait SupportedQueryBenchmarks[G] {
  def apply(query: SupportedQuery): Benchmark[G]
}
