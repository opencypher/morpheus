package org.opencypher.spark.prototype

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.ir.QueryDescriptor

trait SparkCypherPlanner {
  def plan(sparkQueryGraph: QueryDescriptor[Expr]): SupportedQuery
}

