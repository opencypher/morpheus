package org.opencypher.spark.api.frame

import org.opencypher.spark.api.CypherType

trait CypherExpression {
  def cypherType: CypherType
}
