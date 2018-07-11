package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.CypherQueryPlans
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.api.table.FlatRelationalTable
import org.opencypher.okapi.relational.impl.operators.RelationalOperator
import org.opencypher.okapi.trees.TreeNode

case class QueryPlans[T <: FlatRelationalTable[T]](
  logicalPlan: Option[TreeNode[LogicalOperator]],
  relationalPlan: Option[TreeNode[RelationalOperator[T]]]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.map(_.pretty).getOrElse("")

  override def relational: String = relationalPlan.map(_.pretty).getOrElse("")
}

object QueryPlans {
  def empty: CypherQueryPlans = QueryPlans(None, None)
}
