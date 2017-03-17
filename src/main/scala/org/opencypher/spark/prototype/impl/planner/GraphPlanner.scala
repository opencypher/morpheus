package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.spark.SparkCypherGraph
import org.opencypher.spark.prototype.impl.logical
import org.opencypher.spark.prototype.impl.physical.GraphProducer

case class GraphPlannerContext(graph: SparkCypherGraph, globals: GlobalsRegistry)

class GraphPlanner(producer: GraphProducer)
  extends Stage[logical.LogicalOperator, SparkCypherGraph, GraphPlannerContext] {

  import producer._

  def plan(logicalPlan: logical.LogicalOperator)(implicit context: GraphPlannerContext): SparkCypherGraph =
    logicalPlan match {
      case logical.Select(fields, in, _) =>
        plan(in).select(fields.toMap)

      case logical.NodeScan(v, every, _) =>
        context.graph.allNodes(v)

      case logical.Project(it, in, _) =>
        plan(in).project(it)

      case logical.Filter(expr, in, _) => expr match {
          // TODO: Is it justified to treat labels separately?
        case HasLabel(n: Var, ref) =>
          plan(in).labelFilter(n, AllGiven(Set(ref)))
        case e =>
          plan(in).filter(e)
      }

      case logical.ExpandSource(source, rel, types, target, in, _) =>
        val lhs = plan(in)
        // TODO: where is the node label info? We could plan a filter here
        val nodeRhs = context.graph.allNodes(target)
        val relRhs = context.graph.allRelationships(rel).typeFilter(rel, types.relTypes)

        val rhs = relRhs.joinTarget(nodeRhs).on(rel)(target)
        val expanded = lhs.expandSource(rhs).on(source)(rel)

        expanded
      case x =>
        throw new NotImplementedError(s"Can't plan operator $x yet")
    }
}
