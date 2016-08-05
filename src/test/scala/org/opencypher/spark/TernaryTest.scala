package org.opencypher.spark

import org.scalatest.{Matchers, FunSuite}

class TernaryTest extends FunSuite with Matchers {

  test("ternary names") {
    TTrue.toString shouldBe "definitely true"
    TFalse.toString shouldBe "definitely false"
    TMaybe.toString shouldBe "maybe"
  }

  test("is true") {
    TTrue.isTrue shouldBe true
    TFalse.isTrue shouldBe false
    TMaybe.isTrue shouldBe false
  }

  test("maybe true") {
    TTrue.maybeTrue shouldBe true
    TFalse.isTrue shouldBe false
    TMaybe.maybeTrue shouldBe true
  }

  test("is false") {
    TTrue.isFalse shouldBe false
    TFalse.isFalse shouldBe true
    TMaybe.isFalse shouldBe false
  }

  test("maybe false") {
    TTrue.maybeFalse shouldBe false
    TFalse.maybeFalse shouldBe true
    TMaybe.maybeFalse shouldBe true
  }

  test("is definite") {
    TTrue.isDefinite shouldBe true
    TFalse.isDefinite shouldBe true
    TMaybe.isDefinite shouldBe false
  }

  test("is unknown") {
    TTrue.isUnknown shouldBe false
    TFalse.isUnknown shouldBe false
    TMaybe.isUnknown shouldBe true
  }

  test("negation") {
    TTrue.negated shouldBe TFalse
    TFalse.negated shouldBe TTrue
    TMaybe.negated shouldBe TMaybe
  }
}
