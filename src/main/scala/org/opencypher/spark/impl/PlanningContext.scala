package org.opencypher.spark.impl

import org.opencypher.spark.impl.util.SlotSymbolGenerator

class PlanningContext(private val slotNames: SlotSymbolGenerator) {
  def newSlotSymbol(field: StdField): Symbol =
    slotNames.newSlotSymbol(field)
}








