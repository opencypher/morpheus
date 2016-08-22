package org.opencypher.spark.api.frame

import org.opencypher.spark.api.CypherType

trait CypherField {
  self: Serializable =>

  def sym: Symbol
  def cypherType: CypherType
}
