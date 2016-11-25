package org.opencypher.spark.benchmark

import org.opencypher.spark.impl.{StdPropertyGraph, SupportedQuery}

object CypherOnSparkBenchmarks extends SupportedQueryBenchmarks[StdPropertyGraph] {

  override def apply(query: SupportedQuery): Benchmark[StdPropertyGraph] = new Benchmark[StdPropertyGraph] {

    override def name: String = "CoS       "

    override def run(graph: StdPropertyGraph): Outcome = {
      val result = graph.cypher(query).products.toDS

      val (count, checksum) = result.rdd.treeAggregate((0, 0))({
        case ((c, cs), product) => (c + 1, cs ^ product.productElement(0).hashCode())
      }, {
        case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
      })

      new Outcome {
        override def plan: String = result.queryExecution.toString()
        override def computeCount: Long = count
        override def computeChecksum: Int = checksum
      }
    }
  }
}
