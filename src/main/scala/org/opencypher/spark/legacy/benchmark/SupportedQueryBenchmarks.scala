package org.opencypher.spark.legacy.benchmark

import org.opencypher.spark.legacy.impl.SupportedQuery

trait SupportedQueryBenchmarks[G] {
  def apply(query: SupportedQuery): Benchmark[G]
}
