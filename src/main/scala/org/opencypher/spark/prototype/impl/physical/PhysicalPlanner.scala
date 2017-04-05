package org.opencypher.spark.prototype.impl.physical

import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.{ConstantRef, GlobalsRegistry}
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.spark.SparkCypherGraph
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.{PlannerStage, logical}

case class PhysicalPlannerContext(graph: SparkCypherGraph, globals: GlobalsRegistry, constants: Map[ConstantRef, CypherValue])

class PhysicalPlanner
  extends PlannerStage[logical.LogicalOperator, SparkCypherGraph, PhysicalPlannerContext] {

  def plan(logicalPlan: logical.LogicalOperator)(implicit context: PhysicalPlannerContext): SparkCypherGraph = {

    val producer = new GraphProducer(RuntimeContext(context.constants))

    import producer._

    def innerPlan(logicalPlan: logical.LogicalOperator): SparkCypherGraph =
      logicalPlan match {
        case logical.Select(fields, in, _) =>
          innerPlan(in).select(fields)

        case logical.NodeScan(v, every, _) =>
          context.graph.allNodes(v)

        case logical.Project(it, in, _) =>
          innerPlan(in).project(it)

        case logical.Filter(expr, in, _) => expr match {
          // TODO: Is it justified to treat labels separately?
          case HasLabel(n: Var, ref, _) =>
            innerPlan(in).labelFilter(n, AllGiven(Set(ref)))
          case e =>
            innerPlan(in).filter(e)
        }

        // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
        case logical.ExpandSource(source, rel, types, target, in, _) =>
          val lhs = innerPlan(in)
          // TODO: where is the node label info? We could plan a filter here
          val nodeRhs = context.graph.allNodes(target)
          val relRhs = context.graph.allRelationships(rel).typeFilter(rel, types.relTypes)

          val rhs = relRhs.joinTarget(nodeRhs).on(rel)(target)
          val expanded = lhs.expandSource(rhs).on(source)(rel)

          expanded
        case x =>
          throw new NotImplementedError(s"Can't plan operator $x yet")
      }

    innerPlan(logicalPlan)
  }
}
