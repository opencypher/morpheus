package org.opencypher.spark.impl.frame

import org.opencypher.spark.impl.{PlanningContext, StdField, StdSlot}
import org.opencypher.spark.{BinaryRepresentation, CypherFrameSignature}

object StdFrameSignature {
  val empty = new StdFrameSignature
}

class StdFrameSignature(private val map: Map[StdField, StdSlot] = Map.empty)
  extends CypherFrameSignature with (StdField => Option[StdSlot]) {

  override type Field = StdField
  override type Slot = StdSlot

  override def fields: Seq[StdField] = map.keys.toSeq
  override def slots: Seq[StdSlot] = map.values.toSeq.sortBy(_.ordinal)

  override def apply(field: StdField): Option[StdSlot] = map.get(field)

  override def addField(field: StdField)(implicit context: PlanningContext): StdFrameSignature = {
    val entry = field -> StdSlot(context.newSlotSymbol(field), field.cypherType, map.values.size, BinaryRepresentation)
    new StdFrameSignature(map + entry)
  }

  override def aliasFields(aliases: (Symbol, Symbol)*): StdFrameSignature = ???

  override def removeField(sym: Symbol): StdFrameSignature = ???

  override def selectFields(fields: StdField*): (StdFrameSignature, Seq[Slot]) = {
    val thatSet = fields.toSet
    val remainingMap = map collect {
      case (field, slot) if thatSet(field) => field -> slot
    }
    val newOrdinals = remainingMap.values.toSeq.sortBy(_.ordinal).zipWithIndex.toMap
    val newMap = remainingMap map {
      case (field, slot) => field -> slot.copy(ordinal = newOrdinals(slot))
    }
    val retainedOldSlotsSortedByNewOrdinal = newOrdinals.toSeq.sortBy(_._2).map(_._1)
    (new StdFrameSignature(newMap), retainedOldSlotsSortedByNewOrdinal)
  }
}
