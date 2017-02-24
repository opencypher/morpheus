package org.opencypher.spark.prototype

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.ir.CypherQuery

trait SparkCypherPlanner {
  def plan(sparkQueryGraph: CypherQuery[Expr]): SupportedQuery
}

