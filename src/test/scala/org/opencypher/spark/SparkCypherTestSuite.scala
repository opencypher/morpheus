package org.opencypher.spark

import org.junit.runner.RunWith
import org.opencypher.spark.impl.instances.spark.SparkCypherInstances
import org.opencypher.spark.support.{GraphMatchingTestSupport, RecordMatchingTestSupport}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class SparkCypherTestSuite
  extends BaseTestSuite
    with SparkTestSession.Fixture
    with GraphMatchingTestSupport
    with SparkCypherInstances
    with RecordMatchingTestSupport
