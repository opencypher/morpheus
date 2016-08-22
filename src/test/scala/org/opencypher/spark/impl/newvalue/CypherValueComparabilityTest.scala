package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue.companion

class CypherValueComparabilityTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("should compare MAP values correctly") {
    verifyComparability(MAP_valueGroups)
  }

  test("should compare LIST values correctly") {
    verifyComparability(LIST_valueGroups)
  }

  test("should compare STRING values correctly") {
    verifyComparability(STRING_valueGroups)
  }

  test("should compare BOOLEAN values correctly") {
    verifyComparability(BOOLEAN_valueGroups)
  }

  test("should compare INTEGER values correctly") {
    verifyComparability(INTEGER_valueGroups)
  }

  test("should compare FLOAT values correctly") {
    verifyComparability(FLOAT_valueGroups)
  }

  test("should compare NUMBER values correctly") {
    verifyComparability(NUMBER_valueGroups)
  }

  test("should compare ANY values correctly") {
    verifyComparability(ANY_valueGroups)
  }

  private def verifyComparability[V <: CypherValue : CypherValueCompanion](values: ValueGroups[V]): Unit = {
    values.flatten.foreach { v =>
      tryCompare(v, v) should be(if (companion[V].containsNull(v)) None else Some(0))
    }

    values.indexed.zip(values.indexed).foreach { entry =>
      val ((leftIndex, leftValue), (rightIndex, rightValue)) = entry
      val cmp = tryCompare(leftValue, rightValue)

      // direction 1: knowing we have the same value
      val isSameValue = leftIndex == rightIndex
      if (isSameValue)
        cmp should equal(if (companion[V].containsNull(leftValue) || companion[V].containsNull(rightValue)) None else Some(0))
      else
        cmp should not equal Some(0)

      // direction 2: knowing the values are comparable
      if (cmp.nonEmpty && cmp.get <= 0) {
        (leftIndex <= rightIndex) should be(true)
        (companion[V].orderability.compare(leftValue, rightValue) <= 0) should be(true)
      }
    }
  }

  private def tryCompare[V <: CypherValue : CypherValueCompanion](a: V, b: V): Option[Int] = {
    companion[V].comparability(a, b)
  }
}
