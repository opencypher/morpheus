/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.logical.LogicalExternalGraph
import org.opencypher.caps.impl.spark.physical.operators._
import org.opencypher.caps.test.CAPSTestSuite

class PhysicalOptimizerTest extends CAPSTestSuite {
  val emptyRecords = CAPSRecords.empty(RecordHeader.empty)
  val emptyGraph = LogicalExternalGraph("foo", URI.create("example.com"), Schema.empty)

  test("Test insert Cache operators") {
    val plan = CartesianProduct(
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("C", CTNode)
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("B", CTNode)
        ),
        RecordHeader.empty
      ),
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("C", CTNode)
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph, Var("B", CTNode)
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
              Start(emptyRecords, emptyGraph), emptyGraph, Var("C", CTNode)
            ),
            Scan(
              Start(emptyRecords, emptyGraph), emptyGraph, Var("B", CTNode)
            ), RecordHeader.empty
          )
        ),
        Cache(
          CartesianProduct(
            Scan(
              Start(emptyRecords, emptyGraph), emptyGraph, Var("C", CTNode)
            ),
            Scan(
              Start(emptyRecords, emptyGraph), emptyGraph, Var("B", CTNode)
            ), RecordHeader.empty
          )
        ), RecordHeader.empty
      )
    )
  }
}
