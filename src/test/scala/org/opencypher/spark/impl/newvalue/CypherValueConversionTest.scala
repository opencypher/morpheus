package org.opencypher.spark.impl.newvalue

import org.scalatest.{FunSuite, Matchers}

//TODO: class CypherValueConversionTest extends StdTestSuite with CypherValue.Conversion {
class CypherValueConversionTest extends FunSuite with Matchers with CypherValue.Conversion {

  test("Can convert to CypherInteger") {
    convert(2) should equal(CypherInteger(2L))
    convert(4L) should equal(CypherInteger(4L))
  }

  test("Can convert to CypherDouble") {
    convert(2.0f) should equal(CypherFloat(2.0d))
    convert(3.0) should equal(CypherFloat(3.0d))
  }

  private def convert[T](v: T)(implicit ev: T => CypherValue): CypherValue = ev(v)
}
