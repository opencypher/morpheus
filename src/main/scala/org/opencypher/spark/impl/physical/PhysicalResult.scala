package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords}

case class PhysicalResult(records: SparkCypherRecords, graphs: Map[String, SparkCypherGraph]) {
  def mapRecordsWithDetails(f: SparkCypherRecords => SparkCypherRecords): PhysicalResult =
    copy(records = f(records.withDetails))
}

