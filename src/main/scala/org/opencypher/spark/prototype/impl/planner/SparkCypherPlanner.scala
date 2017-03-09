package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir.CypherQuery

trait SparkCypherPlanner {
  def plan(sparkQueryGraph: CypherQuery[Expr]): SupportedQuery
}

