package org.opencypher.spark.impl.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkCypherSession}

final class SparkCypherSessionImpl(val sparkSession: SparkSession) extends SparkCypherSession
