package org.opencypher.spark.prototype

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.ir.QueryModel

trait SparkCypherPlanner {
  def plan(sparkQueryGraph: QueryModel[Expr]): SupportedQuery
}

