package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.logical.LogicalExternalGraph
import org.opencypher.caps.impl.spark.physical.operators.{Cache, CartesianProduct, Scan, Start}
import org.opencypher.caps.test.CAPSTestSuite

class PhysicalOptimizerTest extends CAPSTestSuite {
  val emptyRecords = CAPSRecords.empty(RecordHeader.empty)
  val emptyGraph = LogicalExternalGraph("foo", URI.create("example.com"), Schema.empty)

  test("Test insert Cache operators") {
    val plan = CartesianProduct(
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("C")(CTNode)
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("B")(CTNode)
        ),
        RecordHeader.empty
      ),
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("C")(CTNode)
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("B")(CTNode)
        ),
        RecordHeader.empty
      ),
      RecordHeader.empty
    )

    implicit val context = PhysicalOptimizerContext()
    val rewrittenPlan = new PhysicalOptimizer().process(plan)

    rewrittenPlan should equal(
      CartesianProduct(
        Cache(
          CartesianProduct(
            Scan(
              Cache(
                Start(emptyRecords, emptyGraph)
              ),
              emptyGraph, Var("C")(CTNode)
            ),
            Scan(
              Cache(
                Start(emptyRecords, emptyGraph)
              ),
              emptyGraph, Var("B")(CTNode)
            ), RecordHeader.empty
          )
        ),
        Cache(
          CartesianProduct(
            Scan(
              Cache(
                Start(emptyRecords, emptyGraph)
              ),
              emptyGraph, Var("C")(CTNode)
            ),
            Scan(
              Cache(
                Start(emptyRecords, emptyGraph)
              ),
              emptyGraph, Var("B")(CTNode)
            ), RecordHeader.empty
          )
        ), RecordHeader.empty
      )
    )
  }
}
