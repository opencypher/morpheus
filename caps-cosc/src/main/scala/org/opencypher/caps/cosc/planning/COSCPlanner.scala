package org.opencypher.caps.cosc.planning

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.cosc.COSCRecords
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator
import org.opencypher.caps.ir.api.util.DirectCompilationStage

case class COSCPlannerContext(
  resolver: URI => PropertyGraph,
  records: COSCRecords,
  parameters: Map[String, CypherValue]) {

}

class COSCPlanner extends DirectCompilationStage[FlatOperator, COSCOperator, COSCPlannerContext]{

  override def process(input: FlatOperator)(implicit context: COSCPlannerContext): COSCOperator = {
    println("Physical planning starts here ...")
    ???
  }
}
