package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.ir.SolvedQueryModel
import org.opencypher.spark.prototype.api.ir.pattern.EveryNode
import org.opencypher.spark.prototype.impl.logical.NodeScan

class LogicalOperatorFactory(implicit context: LogicalPlannerContext) {

  private val schema = context.schema

  def nodeScan(node: Var, nodeDef: EveryNode)(solved: SolvedQueryModel[Expr]) = {
    val signature = ???
    NodeScan(node, nodeDef, signature)(solved)
  }
}
