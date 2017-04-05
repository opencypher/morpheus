package org.opencypher.spark.api.frame

import org.opencypher.spark.prototype.api.types.CypherType

trait CypherExpression {
  def cypherType: CypherType
}
