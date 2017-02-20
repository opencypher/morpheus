package org.opencypher.spark.impl.prototype

import org.opencypher.spark.impl.SupportedQuery

trait SparkCypherPlanner {
  def plan(sparkQueryGraph: QueryRepresentation[Expr]): SupportedQuery
}

