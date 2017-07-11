package org.opencypher.spark.support

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait RecordMatchingTestSupport {

  self: BaseTestSuite with SparkTestSession.Fixture =>

  implicit class RecordMatcher(records: SparkCypherRecords) {
    def shouldMatch(expectedRecords: SparkCypherRecords): Assertion = {
      records.header should equal(expectedRecords.header)

      val actualData = records.toLocalIterator.asScala.toSet
      val expectedData = expectedRecords.toLocalIterator.asScala.toSet
      actualData should equal(expectedData)
    }

    def shouldMatchOpaquely(expectedRecords: SparkCypherRecords): Assertion = {
      RecordMatcher(projected(records)) shouldMatch projected(expectedRecords)
    }

    private def projected(records: SparkCypherRecords): SparkCypherRecords = {
      val newSlots = records.header.slots.map(_.content).map {
        case slot: FieldSlotContent => OpaqueField(slot.field)
        case slot: ProjectedExpr => OpaqueField(Var(slot.expr.withoutType)(slot.cypherType))
      }
      val newHeader = RecordHeader.from(newSlots: _*)
      val newData = records.data.toDF(newHeader.internalHeader.columns: _*)
      SparkCypherRecords.create(newHeader, newData)(records.space)
    }
  }
}
