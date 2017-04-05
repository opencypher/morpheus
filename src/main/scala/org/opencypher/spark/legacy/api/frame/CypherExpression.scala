package org.opencypher.spark.legacy.api.frame

import org.opencypher.spark.prototype.api.types.CypherType

trait CypherExpression {
  def cypherType: CypherType
}
