package org.opencypher.spark.impl

import org.apache.spark.sql.{Dataset, Encoder}
import org.opencypher.spark.api._

abstract class StdCypherFrame[Out](sig: StdFrameSignature)
  extends CypherFrame[Out] {

  override type Frame = StdCypherFrame[Out]
  override type RuntimeContext = StdRuntimeContext
  override type Field = StdField
  override type Slot = StdSlot
  override type Signature = StdFrameSignature

  // we need these overrides to work around a highlighting bug in the intellij scala plugin

  override def signature: StdFrameSignature = sig
  override def fields: Seq[StdField] = signature.fields
  override def slots: Seq[StdSlot] = signature.slots

  // Spark renames columns when we convert between Dataset types.
  // To keep track of them we rename them explicitly after each step.
  def alias[T](input: Dataset[T])(implicit encoder: Encoder[T]): Dataset[T] = {
    input.toDF(signature.slotNames: _*).as(encoder)
  }
}

case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField {
  // TODO: Properly escape here
  val columnSym: Symbol = Symbol(sym.name.filterNot(_ == '.').filterNot( _ == '(').filterNot(_ == ')'))
}

case class StdSlot(sym: Symbol, cypherType: CypherType, ordinal: Int, representation: Representation) extends CypherSlot
