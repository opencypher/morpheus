package org.opencypher.caps.test.fixture

import org.apache.spark.sql.SparkSession
import org.opencypher.caps.test.{BaseTestSuite, TestSparkSession}

trait SparkSessionFixture extends BaseTestFixture {
  self: BaseTestSuite =>

  implicit val session: SparkSession = TestSparkSession.instance
}
