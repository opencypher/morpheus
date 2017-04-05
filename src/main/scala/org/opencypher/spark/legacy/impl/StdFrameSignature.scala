package org.opencypher.spark.legacy.impl

import org.opencypher.spark.legacy.api._
import org.opencypher.spark.legacy.api.frame.{CypherFrameSignature, Representation}
import org.opencypher.spark.legacy.impl.StdFrameSignature.FieldAlreadyExists
import org.opencypher.spark.legacy.impl.error.StdErrorInfo
import org.opencypher.spark.legacy.impl.verify.Verification

import scala.language.postfixOps

object StdFrameSignature {
  val empty = new StdFrameSignature

  final case class FieldAlreadyExists(sym: Symbol)(implicit info: StdErrorInfo)
    extends Verification.Error(s"Field '${sym.name}' already exists")(info)
}

class StdFrameSignature(private val fieldMap: Map[StdField, StdSlot] = Map.empty)
  extends CypherFrameSignature with Verification with StdErrorInfo.Implicits {

  override type Field = StdField
  override type Slot = StdSlot

  override def fields: Seq[StdField] = fieldMap.keys.toSeq.sortBy(f => slot(f.sym).get.ordinal)
  override def slots: Seq[StdSlot] = fieldMap.values.toSeq.sortBy(_.ordinal)

  def slotNames: Seq[String] = slots.map(_.sym.name)

  override def field(sym: Symbol): Option[StdField] = fieldMap.keys.find(_.sym == sym)

  override def slot(sym: Symbol): Option[StdSlot] =
    fieldMap.collectFirst { case (field, slot) if field.sym == sym => slot }

  def fieldSlot(field: StdField): Option[StdSlot] = fieldMap.get(field)

  override def dropField(field: Symbol): StdFrameSignature = {
    val filtered = fieldMap.filterKeys(_.sym != field)
    val corrected = filtered.zipWithIndex.map {
      case ((f, s), i) => f -> s.copy(ordinal = i)
    }
    new StdFrameSignature(corrected)
  }

  def fieldOrdinals: IndexedSeq[(Symbol, Int)] = {
    fieldMap.map {
      case (f, s) => f.sym -> s.ordinal
    }.toIndexedSeq
  }

  override def addField(symbol: TypedSymbol)(implicit context: PlanningContext): (Field, StdFrameSignature) = {
    val field = StdField(symbol)

    requireNoSuchFieldExists(field.sym)

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

  override def aliasField(alias: Alias): (StdField, StdFrameSignature) = {
    val (oldSym, newSym) = alias

    requireNoSuchFieldExists(newSym)

    val oldField = obtain(field)(oldSym)
    val newField = oldField.copy(sym = newSym)

    newField -> new StdFrameSignature(fieldMap - oldField + (newField -> fieldMap(oldField)))
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

  override def upcastField(typedSym: TypedSymbol): (StdField, StdFrameSignature) = {
    val (sym, newType) = typedSym
    val oldField = obtain(field)(sym)
    val oldType = oldField.cypherType

    // We do an additional assert here since this is very critical
    if (newType superTypeOf oldType isTrue) {
      val oldSlot = obtain(fieldSlot)(oldField)
      val newField = oldField.copy(cypherType = newType)
      val newSlot = oldSlot.copy(cypherType = newType)
      newField -> new StdFrameSignature(fieldMap - oldField + (newField -> newSlot))
    } else
      throw new IllegalArgumentException(s"Cannot upcast $oldType to $newType")
  }

  def ++(other: StdFrameSignature): StdFrameSignature = {
    // TODO: Remove var
    var highestSlotOrdinal = fieldMap.values.map(_.ordinal).max
    val otherWithUpdatedOrdinals = other.fieldMap.toList.sortBy {
      case (f, s) => s.ordinal
    }.map {
        case (f, s) =>
          highestSlotOrdinal = highestSlotOrdinal + 1
          f -> s.copy(ordinal = highestSlotOrdinal)
    }
    new StdFrameSignature(fieldMap ++ otherWithUpdatedOrdinals)
  }

  override def toString: String = {
    s"Signature($fieldMap)"
  }

  private def requireNoSuchFieldExists(sym: Symbol) =
    ifExists(field(sym)) failWith FieldAlreadyExists(sym)
}
