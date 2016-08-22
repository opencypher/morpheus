package org.opencypher.spark.api.frame

import org.opencypher.spark.api.CypherType

trait CypherSlot {
  self: Serializable =>

  // Unique name of this slot; fixed at creation
  def sym: Symbol

  // The actual expression whose evaluation result is stored in this slot
  // def expr: Set[CypherExpression]

  // Corresponding data frame representation type

  def cypherType: CypherType

  def representation: Representation

  def ordinal: Int
}
