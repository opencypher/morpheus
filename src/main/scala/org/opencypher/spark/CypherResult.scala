package org.opencypher.spark

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.opencypher.spark.impl.StdRecord

trait CypherResult[T] {
  def frame: CypherFrame[T]

  def toDF: DataFrame
  def toDS: Dataset[T]

  def show(): Unit
}
