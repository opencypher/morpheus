package org.opencypher.spark.legacy.api.frame

import org.apache.spark.sql.Dataset

// A CypherFrame is a frame of cypher records. It doubles as
//
// - plan operator for cypher on spark
// - knows how to produce a concrete cypher result
//
trait OldCypherFrame[Out] {

  // Implementations may specialize
  //
  type Frame <: OldCypherFrame[Out]
  type Field <: CypherField
  type Slot <: CypherSlot
  type RuntimeContext <: CypherRuntimeContext
  type Signature <: CypherFrameSignature

  // This is a two layer construct

  def signature: Signature

  //
  // On the top level, there is the signature of the covered cypher record fields
  //
  def fields: Seq[Field]

  //
  // On the bottom level and somewhat internally, the frame tracks slots that hold
  // the results of evaluating certain expressions
  //
  def slots: Seq[Slot]

  // Expressions are not only evaluated over slots but in a wider context
  //  def parameters: Map[Symbol, CypherValue]

  def run(implicit context: RuntimeContext): Dataset[Out]
}
