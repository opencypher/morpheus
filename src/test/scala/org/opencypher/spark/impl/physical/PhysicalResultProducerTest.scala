package org.opencypher.spark.impl.physical

import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.verify
import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.global.{Label, RelType, TokenRegistry}
import org.opencypher.spark.api.ir.pattern.{AllOf, AnyOf, EveryNode, EveryRelationship}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.impl.logical.NamedLogicalGraph
import org.opencypher.spark.impl.record.SparkCypherRecordsTokens
import org.scalatest.mockito.MockitoSugar

class PhysicalResultProducerTest extends SparkCypherTestSuite with MockitoSugar {

  implicit val space: SparkGraphSpace = new SparkGraphSpace {
    override val session: SparkSession = sparkSession
    override var tokens: SparkCypherRecordsTokens = SparkCypherRecordsTokens(TokenRegistry.fromSchema(Schema.empty))
    override val base: SparkCypherGraph = mock[SparkCypherGraph]
  }

  val producer = new PhysicalResultProducer(context)
  import producer._

  test("node scan") {
    val records = SparkCypherRecords.empty()
    val namedLogicalGraph = NamedLogicalGraph("foo", Schema.empty)

    val prev = PhysicalResult(records, Map("foo" -> space.base))
    val v = Var("n")(CTNode)

    val elements = EveryNode(AllOf(Label("Person"), Label("Employee")))

    prev.nodeScan(namedLogicalGraph, v, elements, RecordHeader.empty)
    verify(space.base).nodes("n", CTNode("Person", "Employee"))
  }

  test("relationship scan") {
    val records = SparkCypherRecords.empty()
    val namedLogicalGraph = NamedLogicalGraph("foo", Schema.empty)

    val prev = PhysicalResult(records, Map("foo" -> space.base))
    val v = Var("e")(CTRelationship)

    val elements = EveryRelationship(AnyOf(RelType("KNOWS")))

    prev.relationshipScan(namedLogicalGraph, v, elements, RecordHeader.empty)
    verify(space.base).relationships("e", CTRelationship("KNOWS"))
  }

}
