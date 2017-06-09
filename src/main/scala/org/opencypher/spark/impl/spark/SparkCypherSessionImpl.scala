package org.opencypher.spark.impl.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.spark.SparkCypherSession

final class SparkCypherSessionImpl(val sparkSession: SparkSession) extends SparkCypherSession
