package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, GlobalsRegistry}
import org.opencypher.spark.api.spark.SparkCypherGraph
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.flat.FlatOperator
import org.opencypher.spark.impl.flat
import org.opencypher.spark.impl.DirectCompilationStage

case class PhysicalPlannerContext(graph: SparkCypherGraph, globals: GlobalsRegistry, constants: Map[ConstantRef, CypherValue])

class PhysicalPlanner
  extends DirectCompilationStage[FlatOperator, SparkCypherGraph, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): SparkCypherGraph = {

    val producer = new GraphProducer(RuntimeContext(context.constants, context.globals))

    import producer._

    def innerPlan(flatPlan: FlatOperator): SparkCypherGraph =
      flatPlan match {
        case flat.Select(fields, in, header) =>
          innerPlan(in).select(fields, header)

        case flat.NodeScan(v, labels, in, header) =>
          // TODO: Recursively plan input tree
          context.graph.allNodes(v)

        case flat.Alias(expr, alias, in, header) =>
          innerPlan(in).alias(expr, alias, header)

        case flat.Filter(expr, in, header) => expr match {
          // TODO: Is it justified to treat labels separately?
          case TrueLit() => innerPlan(in)
          case e =>
            innerPlan(in).filter(e, header)
        }

        // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
        case flat.ExpandSource(source, rel, types, target, in, header) =>
          val lhs = innerPlan(in)
          // TODO: where is the node label info? We could plan a filter here
          val nodeRhs = context.graph.allNodes(target)
          val relRhs = context.graph.allRelationships(rel).typeFilter(rel, types.relTypes, header)

          val rhs = relRhs.joinTarget(nodeRhs).on(rel)(target)
          val expanded = lhs.expandSource(rhs).on(source)(rel)

          expanded
        case x =>
          throw new NotImplementedError(s"Can't plan operator $x yet")
      }

    innerPlan(flatPlan)
  }
}
