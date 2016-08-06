package org.opencypher.spark

import org.apache.spark.sql.SparkSession

object TestSession {
  implicit lazy val session = SparkSession.builder().master("local[4]").getOrCreate()
  implicit lazy val factory = PropertyGraphFactory.create
}
