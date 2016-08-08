package org.opencypher.spark.impl

class SlotNameGenerator {
  private var id = 0

  // TODO: escape weird characters
  def newSlotName(prefix: Symbol): Symbol = {
    id += 1
    Symbol(s"${prefix.name}$id")
  }
}
