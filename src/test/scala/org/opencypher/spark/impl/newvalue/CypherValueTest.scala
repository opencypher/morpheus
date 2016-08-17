package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.StdTestSuite

class CypherValueTest extends StdTestSuite {

  test("construct and deconstruct CypherInteger") {
    CypherInteger(2) match { case CypherInteger(v) => v should be(2) }
    cypherNull[CypherInteger] match { case CypherInteger(v) => fail("Should not match") case null => }
  }

  test("construct and deconstruct CypherFloat") {
    CypherFloat(2.0d) match { case CypherFloat(v) => v should be(2.0d) }
    cypherNull[CypherFloat] match { case CypherFloat(v) => fail("Should not match") case null => }
  }

  test("construct integers and floats and deconstruct as numbers") {
    CypherFloat(1.0) match { case CypherNumber(n) => n should be(1.0) }
    CypherInteger(1) match { case CypherNumber(n) => n should be(1) }
    cypherNull[CypherFloat] match { case CypherNumber(n) => fail("Should not match") case null => }
    cypherNull[CypherInteger] match { case CypherNumber(n) => fail("Should not match") case null => }
    cypherNull[CypherNumber] match { case CypherNumber(n) => fail("Should not match") case null => }
  }
}
