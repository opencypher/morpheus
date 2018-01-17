package org.opencypher.caps.logical.impl

import org.opencypher.caps.api.io.GraphSource
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.ir.api.IRGraph
import org.opencypher.caps.ir.api.expr.Var

final case class LogicalPlannerContext(
    ambientGraphSchema: Schema,
    inputRecordFields: Set[Var],
    resolver: String => GraphSource,
    sourceGraph: IRGraph
) {
  def withSourceGraph(graph: IRGraph): LogicalPlannerContext = copy(sourceGraph = graph)
}
