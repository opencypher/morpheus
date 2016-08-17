package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.api.{False, Maybe, Ternary, True}
import org.opencypher.spark.impl.newvalue.CypherValue.{Companion, companion}
import org.scalatest.{FunSuite, Matchers}

class CypherValueEqualityTest extends FunSuite with Matchers with CypherValue.Conversion with CypherValue.Companions {

  // all equals methods on direct instances use equivalence !!!

  test("INTEGER equality") {
    val values = Seq(Long.MinValue, -1L, 0L, 1L, 2L, 3L, Long.MaxValue)

    values.foreach { v => equality[CypherInteger](v, v) should be(True) }
    values.foreach { v => equality[CypherInteger](cypherNull, v) should be(Maybe) }

    values.foreach { v1 =>
      values.foreach { v2 =>
        equality[CypherInteger](v1, v2) should be(Ternary(v1 == v2))
      }
    }
  }

  test("FLOAT equality") {
    val values = Seq(Long.MinValue, -1L, 0L, 1L, 2L, 3L, Long.MaxValue)

    values.foreach { v => equality[CypherInteger](v, v) should be(True) }
    values.foreach { v => equality[CypherInteger](cypherNull, v) should be(Maybe) }
    values.foreach { v => equality[CypherInteger](v, cypherNull) should be(Maybe) }

    values.foreach { v1 =>
      values.foreach { v2 =>
        equality[CypherInteger](v1, v2) should be(Ternary(v1 == v2))
      }
    }
  }

  test("NUMBER equality") {
    equality[CypherNumber](1L, 1.0d) should be(True)
    equality[CypherNumber](1L, cypherNull) should be(Maybe)
    equality[CypherNumber](2L, 1.0d) should be(False)
    equality[CypherNumber](cypherNull, 1.0d) should be(Maybe)
    equality[CypherNumber](cypherNull, cypherNull) should be(Maybe)
  }

  private def equality[S <: CypherValue : Companion](v1: S, v2: S): Ternary = {
    val cmp1 = companion[S].equality.tryCompare(v1, v2)
    val cmp2 = companion[S].equality.tryCompare(v2, v1)

    (cmp1, cmp2) match {
      case (Some(c1), Some(c2)) =>
        if (c1 < 0) (c2 > 0) should be(true)
        if (c1 > 0) (c2 < 0) should be(true)
        if (c1 == 0) (c2 == 0) should be(true)

      case _ =>
        cmp1 should equal(cmp2)
    }

    def tern(cmp: Option[Int], v1: S, v2: S) =
      cmp match {
        case Some(cmp) if cmp < 0 =>
          companion[S].equality.lteq(v1, v2) should be(true)
          companion[S].equality.lteq(v2, v1) should be(false)
          companion[S].equal(v1, v2) should be(False)
          companion[S].equal(v2, v1) should be(False)
          False

        case Some(cmp) if cmp > 0 =>
          companion[S].equality.lteq(v1, v2) should be(false)
          companion[S].equality.lteq(v2, v1) should be(true)
          companion[S].equal(v1, v2) should be(False)
          companion[S].equal(v2, v1) should be(False)
          False

        case Some(cmp) if cmp == 0 =>
          companion[S].equality.lteq(v1, v2) should be(true)
          companion[S].equality.lteq(v2, v1) should be(true)
          companion[S].equal(v1, v2) should be(True)
          companion[S].equal(v2, v1) should be(True)
          True

        case None =>
          companion[S].equality.lteq(v1, v2) should be(false)
          companion[S].equality.lteq(v2, v1) should be(false)
          companion[S].equal(v1, v2) should be(Maybe)
          companion[S].equal(v2, v1) should be(Maybe)
          Maybe
      }

    val t1 = tern(cmp1, v1, v2)
    val t2 = tern(cmp2, v2, v1)

    t1 should equal(t2)

    t1
  }
}
