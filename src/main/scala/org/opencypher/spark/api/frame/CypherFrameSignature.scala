package org.opencypher.spark.api.frame

import org.opencypher.spark.api._
import org.opencypher.spark.impl.PlanningContext

trait CypherFrameSignature {

  type Field <: CypherField
  type Slot <: CypherSlot

  def field(sym: Symbol): Option[Field]
  def slot(name: Symbol): Option[Slot]

  def addField(symbol: TypedSymbol)(implicit context: PlanningContext): (Field, CypherFrameSignature)
  def addFields(symbols: TypedSymbol*)(implicit console: PlanningContext): (Seq[Field], CypherFrameSignature)

  def upcastField(symbol: TypedSymbol): (Field, CypherFrameSignature)
  def aliasField(alias: Alias): (Field, CypherFrameSignature)
  def selectFields(fields: Symbol*): (Seq[Slot], CypherFrameSignature)

  def slots: Seq[Slot]
  def fields: Seq[Field]
}
