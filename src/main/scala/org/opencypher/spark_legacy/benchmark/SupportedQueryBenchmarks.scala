package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark_legacy.impl.SupportedQuery

trait SupportedQueryBenchmarks[G] {
  def apply(query: SupportedQuery): Benchmark[G]
}
