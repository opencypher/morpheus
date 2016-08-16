package org.opencypher.spark.api

import org.opencypher.spark.impl.PlanningContext

trait CypherFrameSignature {

  type Field <: CypherField
  type Slot <: CypherSlot

  def field(sym: Symbol): Field
  def slot(name: Symbol): Slot

  def addField(symbol: TypedSymbol)(implicit context: PlanningContext): (Field, CypherFrameSignature)
  def addFields(symbols: TypedSymbol*)(implicit console: PlanningContext): (Seq[Field], CypherFrameSignature)
  def upcastField(symbol: Symbol, newType: CypherType): (Field, CypherFrameSignature)
  def aliasField(oldField: Symbol, newField: Symbol): (Field, CypherFrameSignature)
  def selectFields(fields: Symbol*): (Seq[Slot], CypherFrameSignature)

  def slots: Seq[Slot]
  def fields: Seq[Field]
}
