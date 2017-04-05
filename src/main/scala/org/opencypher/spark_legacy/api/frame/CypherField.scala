package org.opencypher.spark_legacy.api.frame

import org.opencypher.spark.api.types.CypherType

trait CypherField {
  self: Serializable =>

  def sym: Symbol
  def cypherType: CypherType
}
