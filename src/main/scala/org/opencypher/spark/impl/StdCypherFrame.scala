package org.opencypher.spark.impl

import org.opencypher.spark._

abstract class StdCypherFrame[Out](_fields: Seq[StdField], _slots: Seq[StdSlot]) extends CypherFrame[Out] {

  override type Frame = StdCypherFrame[Out]
  override type RuntimeContext = StdRuntimeContext
  override type Field = StdField
  override type Slot = StdSlot

  def fieldSlots = fields.zip(slots)

  // we need these overrides to work around a highlighting bug in the intellij scala plugin

  override def fields: Seq[StdField] = _fields
  override def slots: Seq[StdSlot] = _slots
}

case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField with Serializable {
  // TODO: Properly escape here
  val columnSym: Symbol = Symbol(sym.name.replace('.', '-'))
}

case class StdSlot(sym: Symbol, cypherType: CypherType, representation: Representation) extends CypherSlot with Serializable
