package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.impl.record.InternalHeader

final case class RecordHeader(internalHeader: InternalHeader) {

  def ++(other: RecordHeader): RecordHeader =
    copy(internalHeader ++ other.internalHeader)

  def indexOf(content: SlotContent): Option[Int] = slots.find(_.content == content).map(_.index)
  def slots: IndexedSeq[RecordSlot] = internalHeader.slots
  def fields: Set[Var] = internalHeader.fields

  def slotsFor(expr: Expr, cypherType: CypherType): Seq[RecordSlot] =
    internalHeader.slotsFor(expr, cypherType)

  def slotsFor(expr: Expr): Seq[RecordSlot] =
    internalHeader.slotsFor(expr)

  def slotFor(variable: Var): RecordSlot = slotsFor(variable).headOption.getOrElse(???)
  def slotsFor(names: String*): Seq[RecordSlot] = names.map(n => slotFor(Var(n)))

  def mandatory(slot: RecordSlot): Boolean =
    internalHeader.mandatory(slot)

  def sourceNode(rel: Var): RecordSlot = slotsFor(StartNode(rel)).headOption.getOrElse(???)
  def targetNode(rel: Var): RecordSlot = slotsFor(EndNode(rel)).headOption.getOrElse(???)
  def typeId(rel: Expr): RecordSlot = slotsFor(TypeId(rel)).headOption.getOrElse(???)
}

object RecordHeader {

  def empty: RecordHeader =
    RecordHeader(InternalHeader.empty)

  def from(contents: SlotContent*): RecordHeader =
    RecordHeader(contents.foldLeft(InternalHeader.empty) { case (header, slot) => header + slot })
}
