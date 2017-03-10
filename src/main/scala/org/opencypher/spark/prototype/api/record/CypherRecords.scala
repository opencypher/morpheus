package org.opencypher.spark.prototype.api.record

trait CypherRecords {
  type Data
  type Records <: CypherRecords
  type Header <: RecordsHeader

  def header: Header
  def data: Data

  def columns: IndexedSeq[String]
  def column(slot: RecordSlot): String
}
