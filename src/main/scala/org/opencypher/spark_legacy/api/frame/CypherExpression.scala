package org.opencypher.spark_legacy.api.frame

import org.opencypher.spark.api.types.CypherType

trait CypherExpression {
  def cypherType: CypherType
}
