package org.opencypher.spark.impl

import org.opencypher.spark.api._

object StdFrameSignature {
  val empty = new StdFrameSignature
}

// TODO: Use verify instead of throwing
// TODO: Check duplicate field
class StdFrameSignature(private val fieldMap: Map[StdField, StdSlot] = Map.empty)
  extends CypherFrameSignature {

  override type Field = StdField
  override type Slot = StdSlot

  override def fields: Seq[StdField] = fieldMap.keys.toSeq
  override def slots: Seq[StdSlot] = fieldMap.values.toSeq.sortBy(_.ordinal)

  def slotNames: Seq[String] = slots.map(_.sym.name)

  override def field(sym: Symbol): StdField =
    fieldMap.keys.find(_.sym == sym).getOrElse(throw new IllegalArgumentException(s"Unknown field: $sym"))

  def slot(field: StdField): StdSlot =
    fieldMap.getOrElse(field, throw new IllegalArgumentException(s"Unknown field: $field"))

  override def slot(sym: Symbol): StdSlot =
    fieldMap.collectFirst {
      case (field, slot) if field.sym == sym => slot
    }.getOrElse(throw new IllegalArgumentException(s"Unknown slot: $sym"))

  override def addField(symbol: TypedSymbol)(implicit context: PlanningContext): (Field, StdFrameSignature) = {
    val field = StdField(symbol)
    val representation = Representation.forCypherType(field.cypherType)
    val slot = StdSlot(context.newSlotSymbol(field), field.cypherType, fieldMap.values.size, representation)
    field -> new StdFrameSignature(fieldMap + (field -> slot))
  }

  override def addFields(symbols: TypedSymbol*)
                        (implicit console: PlanningContext): (Seq[StdField], StdFrameSignature) = {
    symbols.foldLeft(Seq.empty[StdField] -> this) {
      case ((seq, sig), symbol) =>
        val (newField, newSig) = sig.addField(symbol)
        (seq :+ newField) -> newSig
    }
  }

  override def aliasField(oldField: Symbol, newField: Symbol): (StdField, StdFrameSignature) = {
    var copy: StdField = null
    val newMap = fieldMap.map {
      case (f, s) if f.sym == oldField =>
        copy = f.copy(sym = newField)
        copy -> s
      case t => t
    }
    copy -> new StdFrameSignature(newMap)
  }

  override def selectFields(fields: Symbol*): (Seq[Slot], StdFrameSignature) = {
    val fieldSymbolSet = fields.toSet
    val remainingMap = fieldMap collect {
      case (field, slot) if fieldSymbolSet(field.sym) => field -> slot
    }
    val newOrdinals: Map[StdSlot, Int] = remainingMap.values.toSeq.sortBy(_.ordinal).zipWithIndex.toMap
    val newMap: Map[StdField, StdSlot] = remainingMap map {
      case (field, slot) => field -> slot.copy(ordinal = newOrdinals(slot))
    }
    val retainedOldSlotsSortedByNewOrdinal = newOrdinals.toSeq.sortBy(_._2).map(_._1)
    retainedOldSlotsSortedByNewOrdinal -> new StdFrameSignature(newMap)
  }

  override def upcastField(sym: Symbol, newType: CypherType): (StdField, StdFrameSignature) = {
    val oldField = field(sym)
    val oldType = oldField.cypherType

    // We do an additional assert here since this is very critical
    if (newType superTypeOf oldType isTrue) {
      val oldSlot = slot(oldField)
      val newField = oldField.copy(cypherType = newType)
      val newSlot = oldSlot.copy(cypherType = newType)
      newField -> new StdFrameSignature(fieldMap - oldField + (newField -> newSlot))
    } else
      throw new IllegalArgumentException(s"Cannot upcast $oldType to $newType")
  }


  def ++(other: StdFrameSignature): StdFrameSignature = {
    // TODO: Remove var
    var highestSlotOrdinal = fieldMap.values.map(_.ordinal).max
    val otherWithUpdatedOrdinals = other.fieldMap.map {
      case (f, s) =>
        highestSlotOrdinal = highestSlotOrdinal + 1
        f -> s.copy(ordinal = highestSlotOrdinal)
    }
    new StdFrameSignature(fieldMap ++ otherWithUpdatedOrdinals)
  }

  override def toString: String = {
    s"Signature($fieldMap)"
  }
}
