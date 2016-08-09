package org.opencypher.spark.impl

import org.opencypher.spark._
import org.opencypher.spark.impl.frame.StdFrameSignature

abstract class StdCypherFrame[Out](sig: StdFrameSignature) extends CypherFrame[Out] {

  override type Frame = StdCypherFrame[Out]
  override type RuntimeContext = StdRuntimeContext
  override type Field = StdField
  override type Slot = StdSlot
  override type Signature = StdFrameSignature

  // we need these overrides to work around a highlighting bug in the intellij scala plugin

  override def signature: StdFrameSignature = sig
  override def fields: Seq[StdField] = signature.fields
  override def slots: Seq[StdSlot] = signature.slots
}

case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField {
  // TODO: Properly escape here
  val columnSym: Symbol = Symbol(sym.name.replace('.', '-'))
}

case class StdSlot(sym: Symbol, cypherType: CypherType, ordinal: Int, representation: Representation) extends CypherSlot
