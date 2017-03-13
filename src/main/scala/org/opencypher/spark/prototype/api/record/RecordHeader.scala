package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.impl.record.InternalHeader

final case class RecordHeader(internalHeader: InternalHeader) {

  def slots: IndexedSeq[RecordSlot] = internalHeader.slots
  def fields: Set[Var] = internalHeader.fields

  def slotsFor(expr: Expr, cypherType: CypherType): Traversable[RecordSlot] =
    internalHeader.slotsFor(expr, cypherType)

  def slotsFor(expr: Expr): Traversable[RecordSlot] =
    internalHeader.slotsFor(expr)
}

object RecordHeader {

  def empty: RecordHeader =
    RecordHeader(InternalHeader.empty)

  def from(contents: SlotContent*): RecordHeader =
    RecordHeader(contents.foldLeft(InternalHeader.empty) { case (header, slot) => header + slot })
}
