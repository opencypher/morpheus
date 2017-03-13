package org.opencypher.spark.prototype.api.record

trait CypherRecords {
  type Data
  type Records <: CypherRecords

  def header: RecordHeader
  def data: Data

  def columns: IndexedSeq[String]
  def column(slot: RecordSlot): String
}
