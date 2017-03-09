package org.opencypher.spark.prototype.api.record

import org.apache.spark.sql.DataFrame

trait CypherRecords {
  type Data

  def header: Seq[RecordSlot]
  def data: Data
}

trait SparkCypherRecords extends CypherRecords {
  override type Data = DataFrame

  def toDF = data
}
