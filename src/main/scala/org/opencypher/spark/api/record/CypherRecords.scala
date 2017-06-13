package org.opencypher.spark.api.record

trait CypherRecords {

  type Data
  type Records <: CypherRecords

  def header: RecordHeader
  def data: Data

  def columns: IndexedSeq[String]
  def column(slot: RecordSlot): String

  def shape(node: EmbeddedNode): Records
  def shape(rel: EmbeddedRelationship): Records

  // def shape(slot: String, combinedSlots: String*) = ???

  def compact: Records

  def show(): Unit
}
