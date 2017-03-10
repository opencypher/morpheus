package org.opencypher.spark.prototype.api.record

import org.apache.spark.sql.DataFrame

trait CypherRecords {
  type Data
  type Records <: CypherRecords

  def header: Seq[RecordSlot]
  def data: Data
}

trait SparkCypherRecords extends CypherRecords {
  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords

  def toDF = data
}


