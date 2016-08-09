package org.opencypher.spark.impl.util

import org.opencypher.spark.impl.StdField

class SlotSymbolGenerator {
  private var id = 0

  // TODO: escape weird characters
  def newSlotSymbol(field: StdField): Symbol = {
    id += 1
    Symbol(s"${field.columnSym.name}$id")
  }
}
