package org.opencypher.spark

import org.scalatest.{Matchers, FunSuite}

class TernaryTest extends FunSuite with Matchers {

  test("ternary names") {
    True.toString shouldBe "definitely true"
    False.toString shouldBe "definitely false"
    Maybe.toString shouldBe "maybe"
  }

  test("is true") {
    True.isTrue shouldBe true
    False.isTrue shouldBe false
    Maybe.isTrue shouldBe false
  }

  test("maybe true") {
    True.maybeTrue shouldBe true
    False.isTrue shouldBe false
    Maybe.maybeTrue shouldBe true
  }

  test("is false") {
    True.isFalse shouldBe false
    False.isFalse shouldBe true
    Maybe.isFalse shouldBe false
  }

  test("maybe false") {
    True.maybeFalse shouldBe false
    False.maybeFalse shouldBe true
    Maybe.maybeFalse shouldBe true
  }

  test("is definite") {
    True.isDefinite shouldBe true
    False.isDefinite shouldBe true
    Maybe.isDefinite shouldBe false
  }

  test("is unknown") {
    True.isUnknown shouldBe false
    False.isUnknown shouldBe false
    Maybe.isUnknown shouldBe true
  }

  test("negation") {
    True.negated shouldBe False
    False.negated shouldBe True
    Maybe.negated shouldBe Maybe
  }
}
