package org.opencypher.spark.api.value

import org.opencypher.spark.api.value.CypherValue._
import org.opencypher.spark.{StdTestSuite, TestSession}

class CypherValueEncodingTest extends StdTestSuite with TestSession.Fixture {

  import CypherTestValues._

  test("PATH encoding") {
    val values = PATH_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("RELATIONSHIP encoding") {
    val values = RELATIONSHIP_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("NODE encoding") {
    val values = NODE_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("MAP encoding") {
    val values = MAP_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("LIST encoding") {
    val values = LIST_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("STRING encoding") {
    val values = STRING_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("BOOLEAN encoding") {
    val values = BOOLEAN_valueGroups.flatten
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

  test("ANY encoding") {
    val values = ANY_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }
}
