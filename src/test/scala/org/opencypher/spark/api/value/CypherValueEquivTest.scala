package org.opencypher.spark.api.value

import org.opencypher.spark.api.value.CypherValue.companion

class CypherValueEquivTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("PATH equiv") {
    verifyEquiv(PATH_valueGroups)
  }

  test("RELATIONSHIP equiv") {
    verifyEquiv(RELATIONSHIP_valueGroups)
  }

  test("NODE equiv") {
    verifyEquiv(NODE_valueGroups)
  }

  test("MAP equiv") {
    verifyEquiv(MAP_valueGroups)
  }

  test("LIST equiv") {
    verifyEquiv(LIST_valueGroups)
  }

  test("STRING equiv") {
    verifyEquiv(STRING_valueGroups)
  }

  test("BOOLEAN equiv") {
    verifyEquiv(BOOLEAN_valueGroups)
  }

  test("INTEGER equiv") {
    verifyEquiv(INTEGER_valueGroups)
  }

  test("FLOAT equiv") {
    verifyEquiv(FLOAT_valueGroups)
  }

  test("NUMBER equiv") {
    verifyEquiv(NUMBER_valueGroups)
  }

  test("ANY equiv") {
    verifyEquiv(ANY_valueGroups)
  }

  private def verifyEquiv[V <: CypherValue : CypherValueCompanion](valueGroups: ValueGroups[V]): Unit = {
    valueGroups.flatten.foreach { v =>
      equiv(v, v) should be(true)
      if (! companion[V].isNull(v)) {
        equiv(v, cypherNull) should be(false)
        equiv(cypherNull, v) should be(false)
      }
    }

    equiv[V](cypherNull, cypherNull) should be(true)

    val indexedValueGroups =
      valueGroups
        .zipWithIndex
        .flatMap { case ((group), index) => group.map { v => index -> v } }

    indexedValueGroups.foreach { left =>
      val ((leftIndex, leftValue)) = left
       indexedValueGroups.foreach { right =>
         val ((rightIndex, rightValue)) = right
         val areEquivalent = equiv(leftValue, rightValue)
         val areSame = leftIndex == rightIndex
         areEquivalent should equal(areSame)
       }
    }
  }

  private def equiv[V <: CypherValue : CypherValueCompanion](v1: V, v2: V): Boolean = {
    val b1 = companion[V].equiv(v1, v2)
    val b2 = companion[V].equiv(v2, v1)

//    println(s"$v1 $v2 $b1 $b2")

    b1 should be(b2)
    (v1 == v2) should be(b2)
    (v2 == v1) should be(b2)

    b1
  }
}
