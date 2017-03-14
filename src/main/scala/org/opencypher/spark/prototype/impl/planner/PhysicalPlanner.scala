package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.logical
import org.opencypher.spark.prototype.impl.logical.LogicalOperator
import org.opencypher.spark.prototype.impl.physical.PhysicalOperator

final case class PhysicalPlannerContext(schema: Schema, globalsRegistry: GlobalsRegistry)

class PhysicalPlanner extends Stage[LogicalOperator, PhysicalOperator, PhysicalPlannerContext] {

  override def plan(input: LogicalOperator)(implicit context: PhysicalPlannerContext): PhysicalOperator = {
    val mkPhysical = new PhysicalOperatorProducer()

    input match {
      case logical.NodeScan(node, nodeDef, _) =>
        mkPhysical.nodeScan(node, nodeDef)
    }
  }
}
