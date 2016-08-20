package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue._
import org.opencypher.spark.{StdTestSuite, TestSession}

class CypherValueEncodingTest extends StdTestSuite with TestSession.Fixture {

  import CypherTestValues._

  test("BOOLEAN encoding") {
    val values = BOOLEAN_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("STRING encoding") {
    val values = STRING_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("INTEGER encoding") {
    val values = INTEGER_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("FLOAT encoding") {
    val values = FLOAT_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("NUMBER encoding") {
    val values = NUMBER_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }
}
