package org.opencypher.spark.api.exception

sealed case class SparkCypherException(msg: String) extends RuntimeException(msg)
