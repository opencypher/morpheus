package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords}

case class InternalResult(records: SparkCypherRecords, graphs: Map[String, SparkCypherGraph]) {
  def mapRecords(f: SparkCypherRecords => SparkCypherRecords): InternalResult =
    copy(records = f(records))
}

