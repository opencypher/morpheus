package org.opencypher.spark.prototype.api.spark

import java.util.Collections

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.syntax.header._
import org.opencypher.spark.prototype.impl.spark.{SparkColumnName, SparkSchema}

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

  override def compact = new SparkCypherRecords {

    private lazy val cachedHeader = self.header.update(selectFields)._1

    private lazy val cachedData = {
      val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
      self.data.select(columns: _*)
    }

    override def header = cachedHeader
    override def data = cachedData
  }

  override def show() = toDF().show()
}

object SparkCypherRecords {
  def empty(session: SparkSession, emptyHeader: RecordHeader = RecordHeader.empty) = new SparkCypherRecords {
    override def data = session.createDataFrame(Collections.emptyList[Row](), SparkSchema.from(emptyHeader))
    override def header = emptyHeader
  }
}
