package org.opencypher.spark

import org.opencypher.spark.impl.PlanningContext

trait CypherFrameSignature {

  type Field <: CypherField
  type Slot <: CypherSlot

  def apply(field: Field): Option[Slot]

  def addField(field: Field)(implicit context: PlanningContext): CypherFrameSignature
  def addIntegerField(field: Field)(implicit context: PlanningContext): CypherFrameSignature
  def removeField(sym: Symbol): CypherFrameSignature
  def aliasField(oldField: Symbol, newField: Symbol): (Field, CypherFrameSignature)
  def selectFields(fields: Field*): (CypherFrameSignature, Seq[Slot])

  def slots: Seq[Slot]
  def fields: Seq[Field]
}
