package org.opencypher.spark

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.types.{BinaryType, DataType}

trait Expr

// A CypherFrame is a frame of cypher records. It doubles as
//
// - plan operator for cypher on spark
// - knows how to produce a concrete cypher result
//
trait CypherFrame[Out] {

  // Implementations may specialize
  //
  type Frame <: CypherFrame[Out]
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
  //  def expand(other: Frame)(from: Field, to: Field): Frame // just sketching
}

trait CypherRuntimeContext {
  def session: SparkSession
}

trait CypherField {
  self: Serializable =>

  def sym: Symbol
  def cypherType: CypherType
}

trait CypherSlot {
  self: Serializable =>

  // Unique name of this slot; fixed at creation
  def sym: Symbol

  // The actual expression whose evaluation result is stored in this slot
  // def expr: Set[CypherExpression]

  // Corresponding data frame representation type

  def representation: Representation

  def ordinal: Int
}

sealed trait Representation extends Serializable {
  def dataType: DataType
}

case object BinaryRepresentation extends Representation {
  def dataType = BinaryType
}

final case class EmbeddedRepresentation(dataType: DataType) extends Representation

trait CypherExpression {
  def cypherType: CypherType
}
