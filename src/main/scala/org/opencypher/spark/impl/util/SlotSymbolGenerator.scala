package org.opencypher.spark.impl.util

import org.opencypher.spark.impl.{SparkIdentifier, StdField}

class SlotSymbolGenerator {
  private var id = 0

  def newSlotSymbol(field: StdField): Symbol = {
    id += 1
    (field.column ++ SparkIdentifier(id.toString)).symbol
  }
}
