package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue.{Companion, companion}

class CypherValueEquivTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("INTEGER equiv") {
    verifyEquiv(INTEGER_valueGroups)
  }

  test("FLOAT equiv") {
    verifyEquiv(FLOAT_valueGroups)
  }

  test("NUMBER equiv") {
    verifyEquiv(NUMBER_valueGroups)
  }

  private def verifyEquiv[V <: CypherValue : Companion](values: ValueGroups[V]): Unit = {
    values.flatten.foreach { v =>
      equiv(v, v) should be(true)
      if (! companion[V].isNull(v)) {
        equiv(v, cypherNull) should be(false)
        equiv(cypherNull, v) should be(false)
      }
    }

    equiv[V](cypherNull, cypherNull) should be(true)

    val indexedValues =
      values
        .zipWithIndex
        .flatMap { case ((group), index) => group.map { v => index -> v } }

    indexedValues.zip(indexedValues).foreach { entry =>
      val ((leftIndex, leftValue), (rightIndex, rightValue)) = entry
      val areEquivalent = equiv(leftValue, rightValue)
      val areSame = leftIndex == rightIndex
      areEquivalent should equal(areSame)
    }
  }

  private def equiv[V <: CypherValue : Companion](v1: V, v2: V): Boolean = {
    val b1 = companion[V].equiv(v1, v2)
    val b2 = companion[V].equiv(v2, v1)

    b1 should be(b2)
    (v1 == v2) should be(b2)
    (v2 == v1) should be(b2)

    b1
  }
}
