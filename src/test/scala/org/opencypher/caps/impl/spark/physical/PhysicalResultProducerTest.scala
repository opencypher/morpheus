/**
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

import org.mockito.Mockito.verify
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.ir.api.global.{Label, RelType, TokenRegistry}
import org.opencypher.caps.ir.api.pattern.{AllOf, AnyOf, EveryNode, EveryRelationship}
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.logical.NamedLogicalGraph
import org.opencypher.caps.test.CAPSTestSuite
import org.scalatest.mockito.MockitoSugar

class PhysicalResultProducerTest extends CAPSTestSuite with MockitoSugar {

  override def initialTokens = TokenRegistry.fromSchema(Schema.empty)

  val producer = new PhysicalResultProducer(context)
  import producer._

  test("node scan") {
    val graph = mock[CAPSGraph]
    val records = CAPSRecords.empty()
    val namedLogicalGraph = NamedLogicalGraph("foo", Schema.empty)

    val prev = PhysicalResult(records, Map("foo" -> graph))
    val v = Var("n")(CTNode)

    val elements = EveryNode(AllOf(Label("Person"), Label("Employee")))

    prev.nodeScan(namedLogicalGraph, v, elements, RecordHeader.empty)
    verify(graph).nodes("n", CTNode("Person", "Employee"))
  }

  test("relationship scan") {
    val graph = mock[CAPSGraph]
    val records = CAPSRecords.empty()
    val namedLogicalGraph = NamedLogicalGraph("foo", Schema.empty)

    val prev = PhysicalResult(records, Map("foo" -> graph))
    val v = Var("e")(CTRelationship)

    val elements = EveryRelationship(AnyOf(RelType("KNOWS")))

    prev.relationshipScan(namedLogicalGraph, v, elements, RecordHeader.empty)
    verify(graph).relationships("e", CTRelationship("KNOWS"))
  }

}
