package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.impl.logical.LogicalOperator
import org.opencypher.spark.impl.{DirectCompilationStage, logical}

final case class FlatPlannerContext(schema: Schema, globalsRegistry: GlobalsRegistry)

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.Select(fields, in) =>
        producer.select(fields, process(in))

      case logical.Filter(expr, in) =>
        producer.filter(expr, process(in))

      case logical.NodeScan(node, nodeDef, in) =>
        producer.nodeScan(node, nodeDef, process(in))

      case logical.Project(it, in) =>
        producer.project(it, process(in))

      case logical.ExpandSource(source, rel, types, target, sourceOp, targetOp) =>
        producer.expandSource(source, rel, types, target, process(sourceOp), process(targetOp))

      case logical.LoadGraph(outGraph, source) =>
        producer.planLoadGraph(outGraph, source)

      case x => throw new NotImplementedError(s"Flat planning not done yet for $x")
    }
  }
}
