package org.opencypher.caps.impl.physical

import org.mockito.Mockito.verify
import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.ir.global.{Label, RelType, TokenRegistry}
import org.opencypher.caps.api.ir.pattern.{AllOf, AnyOf, EveryNode, EveryRelationship}
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.logical.NamedLogicalGraph
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
