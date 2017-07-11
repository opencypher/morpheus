package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult}

object ResultBuilder {
  def from(internal: InternalResult): SparkCypherResult = new SparkCypherResult {
    override def records: SparkCypherRecords = internal.records

    // TODO: Track which graph was the 'latest' used one
    override def graph: SparkCypherGraph = internal.graphs.head._2

    override def result(name: String): Option[SparkCypherResult] = internal.graphs.get(name).map { g =>
      new SparkCypherResult {
        override def records: SparkCypherRecords =
          throw new NotImplementedError("Records of stored intermediate result are not tracked!")

        override def graph: SparkCypherGraph = g

        override def result(name: String): Option[SparkCypherResult] = None
      }
    }
  }
}
