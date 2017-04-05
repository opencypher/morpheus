package org.opencypher.spark.legacy.api

import org.apache.spark.sql.{DataFrame, Dataset}
import org.opencypher.spark.legacy.api.frame.CypherFrameSignature

trait CypherResult[T] {

  def signature: CypherFrameSignature

  def toDF: DataFrame
  def toDS: Dataset[T]

  def collectAsScalaList = toDS.collect().toList
  def collectAsScalaSet = toDS.collect().toSet

  def exhaust() = {
    val itr = toDS.toLocalIterator()
    while (itr.hasNext) itr.next()
  }

  def count(): Long = toDS.count()
}


