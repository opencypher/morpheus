package org.opencypher.caps.test.support

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{FieldSlotContent, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.SparkSessionFixture
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait RecordMatchingTestSupport {

  self: BaseTestSuite with SparkSessionFixture =>

  implicit class RecordMatcher(records: CAPSRecords) {
    def shouldMatch(expectedRecords: CAPSRecords): Assertion = {
      records.header should equal(expectedRecords.header)

      val actualData = records.toLocalIterator.asScala.toSet
      val expectedData = expectedRecords.toLocalIterator.asScala.toSet
      actualData should equal(expectedData)
    }

    def shouldMatchOpaquely(expectedRecords: CAPSRecords): Assertion = {
      RecordMatcher(projected(records)) shouldMatch projected(expectedRecords)
    }

    private def projected(records: CAPSRecords): CAPSRecords = {
      val newSlots = records.header.slots.map(_.content).map {
        case slot: FieldSlotContent => OpaqueField(slot.field)
        case slot: ProjectedExpr => OpaqueField(Var(slot.expr.withoutType)(slot.cypherType))
      }
      val newHeader = RecordHeader.from(newSlots: _*)
      val newData = records.data.toDF(newHeader.internalHeader.columns: _*)
      CAPSRecords.create(newHeader, newData)(records.caps)
    }
  }
}
