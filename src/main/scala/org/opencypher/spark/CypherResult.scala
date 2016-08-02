package org.opencypher.spark

import org.apache.spark.sql.{Dataset, Encoder, DataFrame}

trait CypherResult {
  def toDF: DataFrame
  def toDS[T : Encoder](f: CypherRecord => T): Dataset[T]
  def show(): Unit
}
