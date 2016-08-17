package org.opencypher.spark.impl.newvalue

import org.scalatest.{FunSuite, Matchers}

class CypherValueEquivalenceTest extends FunSuite with Matchers with CypherValue.Conversion with CypherValue.Companions {

  // all equals methods on direct instances use equivalence !!!

  import CypherValue.{Companion, companion}

  test("INTEGER equivalence") {
    val values = Seq(Long.MinValue, -1L, 0L, 1L, 2L, 3L, Long.MaxValue)

    values.foreach { v1 =>
      values.foreach { v2 =>
        if (v1 == v2)
          assertEquiv(CypherInteger(v1), CypherInteger(v1))
        else
          assertNotEquiv(CypherInteger(v1), CypherInteger(v2))
      }
    }

    companion[CypherInteger].equiv(cypherNull, cypherNull) should be(true)
  }

  test("FLOAT equivalence") {
    val values = Seq(
      Double.NegativeInfinity, Double.MinValue, -1.0d, 0.0d,
      Double.MinPositiveValue, 1.0d, 2.0d, 3.0d, Double.MaxValue, Double.PositiveInfinity
    )

    values.foreach { v1 =>
      values.foreach { v2 =>
        if (v1 == v2)
          assertEquiv(CypherFloat(v1), CypherFloat(v1))
        else
          assertNotEquiv(CypherFloat(v1), CypherFloat(v2))
      }
    }

    companion[CypherFloat].equiv(Double.NaN, Double.NaN) should be(true)
    companion[CypherFloat].equiv(cypherNull, Double.NaN) should be(true)
    companion[CypherFloat].equiv(Double.NaN, cypherNull) should be(true)
    companion[CypherFloat].equiv(cypherNull, cypherNull) should be(true)
  }

  test("NUMBER equality") {
    assertEquiv[CypherNumber](2.0d, 2L)
    assertNotEquiv[CypherNumber](2.1d, 2L)
  }

  private def assertEquiv[S <: CypherValue : Companion](v1: S, v2: S) = {
    v1 should equal(v2)
    v2 should equal(v1)
    companion[S].equiv(v1, v2) should be(true)
    companion[S].equiv(cypherNull[S], v1) should be(false)
    companion[S].equiv(v2, v1) should be(true)
    companion[S].equiv(v1, cypherNull[S]) should be(false)
    companion[S].equiv(cypherNull[S], v2) should be(false)
    companion[S].equiv(v2, cypherNull[S]) should be(false)
    companion[S].equivalence.compare(cypherNull, v2) > 0 should be(true)
    companion[S].equivalence.compare(v2, cypherNull) < 0 should be(true)
    companion[S].equivalence.compare(cypherNull, v1) > 0 should be(true)
    companion[S].equivalence.compare(v1, cypherNull) < 0 should be(true)
  }

  private def assertNotEquiv[S <: CypherValue : Companion](v1: S, v2: S) = {
    v1 should not equal v2
    v2 should not equal v1
    companion[S].equiv(v1, v2) should be(false)
    companion[S].equiv(cypherNull[S], v1) should be(false)
    companion[S].equiv(v2, v1) should be(false)
    companion[S].equiv(v1, cypherNull[S]) should be(false)
    companion[S].equiv(cypherNull[S], v2) should be(false)
    companion[S].equiv(v2, cypherNull[S]) should be(false)
    companion[S].equivalence.compare(cypherNull, v2) > 0 should be(true)
    companion[S].equivalence.compare(v2, cypherNull) < 0 should be(true)
    companion[S].equivalence.compare(cypherNull, v1) > 0 should be(true)
    companion[S].equivalence.compare(v1, cypherNull) < 0 should be(true)
  }
}
