package org.opencypher.spark.prototype.api.spark

import java.util.Collections

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.spark.SparkSchema

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

object SparkCypherRecords {
  def empty(session: SparkSession, emptyHeader: RecordHeader = RecordHeader.empty) = new SparkCypherRecords {
    override def data = session.createDataFrame(Collections.emptyList[Row](), SparkSchema.from(emptyHeader))
    override def header = emptyHeader
  }
}
