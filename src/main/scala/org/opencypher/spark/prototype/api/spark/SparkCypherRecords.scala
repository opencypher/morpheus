package org.opencypher.spark.prototype.api.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.prototype.api.record._

trait SparkCypherRecords extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords

  override def columns: IndexedSeq[String] =
    header.internalHeader.columns

  override def column(slot: RecordSlot): String =
    header.internalHeader.column(slot)

  def toDF(): Data = {
    data.cache()
  }

  override def show() = toDF().show()
}
