package org.opencypher.spark.prototype.api.record

import org.apache.spark.sql.DataFrame

trait CypherRecords {
  def header: Seq[RecordSlot]

  def toDF: DataFrame
}
