package org.opencypher.spark.api.record

import org.opencypher.spark.api.expr._
import org.opencypher.spark.impl.record.InternalHeader

final case class RecordHeader(internalHeader: InternalHeader) {

  def ++(other: RecordHeader): RecordHeader =
    copy(internalHeader ++ other.internalHeader)

  def indexOf(content: SlotContent): Option[Int] = slots.find(_.content == content).map(_.index)
  def slots: IndexedSeq[RecordSlot] = internalHeader.slots
  def contents: Set[SlotContent] = slots.map(_.content).toSet
  def fields: Set[Var] = internalHeader.fields

  def slotsFor(expr: Expr): Seq[RecordSlot] =
    internalHeader.slotsFor(expr)

  def slotFor(variable: Var): RecordSlot = slotsFor(variable).headOption.getOrElse(???)
  def slotsFor(names: String*): Seq[RecordSlot] =
    names.map(n => internalHeader.slotsByName(n).headOption.getOrElse(???))

  def mandatory(slot: RecordSlot): Boolean =
    internalHeader.mandatory(slot)

  def sourceNode(rel: Var): RecordSlot = slotsFor(StartNode(rel)()).headOption.getOrElse(???)
  def targetNode(rel: Var): RecordSlot = slotsFor(EndNode(rel)()).headOption.getOrElse(???)
  def typeId(rel: Expr): RecordSlot = slotsFor(TypeId(rel)()).headOption.getOrElse(???)

  override def toString = {
    val s = slots
    s"RecordHeader with ${s.size} slots: \n\t ${slots.mkString("\n\t")}"
  }
}

object RecordHeader {

  def empty: RecordHeader =
    RecordHeader(InternalHeader.empty)

  def from(contents: SlotContent*): RecordHeader =
    RecordHeader(contents.foldLeft(InternalHeader.empty) { case (header, slot) => header + slot })
}
