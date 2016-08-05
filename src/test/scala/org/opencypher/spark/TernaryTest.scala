package org.opencypher.spark

import org.scalatest.{Matchers, FunSuite}

class TernaryTest extends FunSuite with Matchers {

  test("Ternary.toString") {
    True.toString shouldBe "definitely true"
    False.toString shouldBe "definitely false"
    Maybe.toString shouldBe "maybe"
  }

  test("Ternary.isTrue") {
    True.isTrue shouldBe true
    False.isTrue shouldBe false
    Maybe.isTrue shouldBe false
  }

  test("Ternary.maybeTrue") {
    True.maybeTrue shouldBe true
    False.isTrue shouldBe false
    Maybe.maybeTrue shouldBe true
  }

  test("Ternary.isFalse") {
    True.isFalse shouldBe false
    False.isFalse shouldBe true
    Maybe.isFalse shouldBe false
  }

  test("Ternary.maybeFalse") {
    True.maybeFalse shouldBe false
    False.maybeFalse shouldBe true
    Maybe.maybeFalse shouldBe true
  }

  test("Ternary.isDefinite") {
    True.isDefinite shouldBe true
    False.isDefinite shouldBe true
    Maybe.isDefinite shouldBe false
  }

  test("Ternary.isUnknown") {
    True.isUnknown shouldBe false
    False.isUnknown shouldBe false
    Maybe.isUnknown shouldBe true
  }

  test("Ternary.negated") {
    True.negated shouldBe False
    False.negated shouldBe True
    Maybe.negated shouldBe Maybe
  }
}
