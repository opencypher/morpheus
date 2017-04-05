package org.opencypher.spark.api.frame

import org.opencypher.spark.prototype.api.types.CypherType

trait CypherField {
  self: Serializable =>

  def sym: Symbol
  def cypherType: CypherType
}
