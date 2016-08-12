package org.opencypher.spark.api

import org.apache.spark.sql.{DataFrame, Dataset}

trait CypherResult[T] {

  def signature: CypherFrameSignature

  def toDF: DataFrame
  def toDS: Dataset[T]

  def collectAsScalaList = toDS.collect().toList
  def collectAsScalaSet = toDS.collect().toSet
}


