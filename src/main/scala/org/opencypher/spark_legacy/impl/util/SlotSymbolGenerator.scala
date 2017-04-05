package org.opencypher.spark_legacy.impl.util

import org.opencypher.spark_legacy.impl.StdField

class SlotSymbolGenerator {
  private var id = 0

  def newSlotSymbol(field: StdField): Symbol = {
    id += 1
    val fieldIdent = field.column
    val idIdent = SparkIdentifier.fromString(id.toString)
    val slotIdent = fieldIdent ++ idIdent
    val slotSymbol = slotIdent.symbol
    slotSymbol
  }
}
