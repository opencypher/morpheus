package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue.companion

class CypherValueComparabilityTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("should order BOOLEAN values correctly") {
    verifyComparability(BOOLEAN_valueGroups)
  }

  test("should order INTEGER values correctly") {
    verifyComparability(INTEGER_valueGroups)
  }

  test("should order FLOAT values correctly") {
    verifyComparability(FLOAT_valueGroups)
  }

  test("should order NUMBER values correctly") {
    verifyComparability(FLOAT_valueGroups)
  }

  private def verifyComparability[V <: CypherValue : CypherValueCompanion](values: ValueGroups[V]): Unit = {
    values.flatten.foreach { v =>
      tryCompare(v, v) should be(Some(0))
    }

    values.indexed.zip(values.indexed).foreach { entry =>
      val ((leftIndex, leftValue), (rightIndex, rightValue)) = entry
      val cmp = tryCompare(leftValue, rightValue)

      // direction 1: knowing we have the same value
      val isSameValue = leftIndex == rightIndex
      if (isSameValue)
        cmp should equal(Some(0))
      else
        cmp should not equal Some(0)

      // direction 2: knowing the values are partially ordered
      if (lteq(leftValue, rightValue)) {
        (leftIndex <= rightIndex) should be(true)
        (companion[V].orderability.compare(leftValue, rightValue) <= 0) should be(true)
      }
    }
  }

  private def lteq[V <: CypherValue : CypherValueCompanion](v1: V, v2: V): Boolean = {
    val a = material(v1)
    val b = material(v2)
    val comparability = companion[V].comparability
    val result = comparability.lteq(a, b)

    result
  }

  private def tryCompare[V <: CypherValue : CypherValueCompanion](v1: V, v2: V): Option[Int] = {
    val a = material(v1)
    val b = material(v2)
    val comparability = companion[V].comparability
    val result = comparability.tryCompare(a, b)

    result match {
      case Some(cmp) if cmp == 0 =>
        comparability.lteq(a, b) should be(true)
        comparability.lteq(b, a) should be(true)

      case Some(cmp) if cmp < 0 =>
        comparability.lteq(a, b) should be(true)
        comparability.lteq(b, a) should be(false)

      case Some(cmp) if cmp > 0 =>
        comparability.lteq(a, b) should be(false)
        comparability.lteq(b, a) should be(true)

      case None =>
        comparability.lteq(a, b) should be(false)
        comparability.lteq(b, a) should be(false)
    }
    result
  }

  private def material[V <: CypherValue : CypherValueCompanion](v: V) =
    companion[V].materialValue(v)
}
