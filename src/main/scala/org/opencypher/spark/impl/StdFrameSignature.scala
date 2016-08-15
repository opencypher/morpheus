package org.opencypher.spark.impl

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._
import org.opencypher.spark.api._

object StdFrameSignature {
  val empty = new StdFrameSignature
}

class StdFrameSignature(private val map: Map[StdField, StdSlot] = Map.empty)
  extends CypherFrameSignature {

  override type Field = StdField
  override type Slot = StdSlot

  override def fields: Seq[StdField] = map.keys.toSeq
  override def slots: Seq[StdSlot] = map.values.toSeq.sortBy(_.ordinal)

  def slotNames: Seq[String] = slots.map(_.sym.name)

  override def field(sym: Symbol): StdField =
    map.keys.find(_.sym == sym).getOrElse(throw new IllegalArgumentException(s"Unknown field: $sym"))

  override def slot(field: StdField): StdSlot =
    map.getOrElse(field, throw new IllegalArgumentException(s"Unknown field: $field"))

  override def slot(sym: Symbol): StdSlot =
    map.collectFirst {
      case (field, slot) if field.sym == sym => slot
    }.getOrElse(throw new IllegalArgumentException(s"Unknown slot: $sym"))

  override def addField(pair: (Symbol, CypherType))(implicit context: PlanningContext): (Field, StdFrameSignature) = {
    val field = StdField(pair)
    val slot = StdSlot(context.newSlotSymbol(field), field.cypherType, map.values.size, Representation.sparkReprForCypherType(field.cypherType))
    field -> new StdFrameSignature(map + (field -> slot))
  }


  override def aliasField(oldField: Symbol, newField: Symbol): (StdField, StdFrameSignature) = {
    var copy: StdField = null
    val newMap = map.map {
      case (f, s) if f.sym == oldField =>
        copy = f.copy(sym = newField)
        copy -> s
      case t => t
    }
    copy -> new StdFrameSignature(newMap)
  }

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
    new StdFrameSignature(newMap) -> retainedOldSlotsSortedByNewOrdinal
  }

  def ++(other: StdFrameSignature): StdFrameSignature = {
    // TODO: Remove var
    var highestSlotOrdinal = map.values.map(_.ordinal).max
    val otherWithUpdatedOrdinals = other.map.map {
      case (f, s) =>
        highestSlotOrdinal = highestSlotOrdinal + 1
        f -> s.copy(ordinal = highestSlotOrdinal)
    }
    new StdFrameSignature(map ++ otherWithUpdatedOrdinals)
  }

  override def toString: String = {
    s"Signature($map)"
  }
}
