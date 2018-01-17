package org.opencypher.caps.logical

import org.opencypher.caps.ir.api.SolvedQueryModel
import org.opencypher.caps.ir.impl.IrTestSuite
import org.opencypher.caps.logical.impl.{LogicalExternalGraph, Start}

abstract class LogicalTestSuite extends IrTestSuite {

  def leafPlan: Start =
    Start(LogicalExternalGraph(testGraph.name, testGraph.uri, testGraph.schema), Set.empty, SolvedQueryModel.empty)

}
