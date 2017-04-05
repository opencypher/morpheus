package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.impl.{PlannerStage, logical}
import org.opencypher.spark.impl.logical.LogicalOperator

final case class FlatPlannerContext(schema: Schema, globalsRegistry: GlobalsRegistry)

class FlatPlanner extends PlannerStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def plan(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val mkPhysical = new FlatOperatorProducer()

    input match {

      case logical.Select(fields, in, _) =>
        val remaining = fields.toSet
        mkPhysical.select(fields, plan(in))

      case logical.Filter(expr, in, _) =>
        mkPhysical.filter(expr, plan(in))

      case logical.NodeScan(node, nodeDef, _) =>
        mkPhysical.nodeScan(node, nodeDef)

      case logical.Project(it, in, _) =>
        mkPhysical.project(it, plan(in))

      case logical.ExpandSource(source, rel, types, target, in, _) =>
        mkPhysical.expandSource(source, rel, types, target, plan(in))
    }
  }
}
