package org.opencypher.spark.support

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
  }
}
