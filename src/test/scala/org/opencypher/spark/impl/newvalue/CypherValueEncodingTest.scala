package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue._
import org.opencypher.spark.{StdTestSuite, TestSession}

class CypherValueEncodingTest extends StdTestSuite with TestSession.Fixture {

  test("Can encode cypher integer as cypher value via kryo") {
    val values = Seq(Long.MinValue, -3L, 0L, 1L, 2L, Long.MaxValue).map(CypherInteger(_)) :+ cypherNull
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("Can encode cypher float as cypher value via kryo") {
    val values = Seq(Double.NegativeInfinity, Double.MinValue -3.0d, 0.0d, Double.MinPositiveValue, 1.0d, 2.0d, Double.MaxValue, Double.PositiveInfinity, Double.NaN).map(CypherFloat(_)) :+ cypherNull
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("Can encode cypher numbers as cypher value via kryo") {
    val values = Seq(CypherFloat(1.09), CypherInteger(1), CypherFloat(Double.NaN), CypherInteger(Long.MaxValue))
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }
}
