package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.impl.newvalue.CypherValue.{Companion, companion}

import scala.annotation.tailrec
import scala.util.Random

class CypherValueOrderabilityTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("should order INTEGER values correctly") {
    verifyOrderabilityReflexivity(INTEGER_valueGroups)
    verifyOrderabilityOrder(INTEGER_valueGroups)
  }

  test("should order FLOAT values correctly") {
    verifyOrderabilityReflexivity(FLOAT_valueGroups)
    verifyOrderabilityOrder(FLOAT_valueGroups)
  }

  test("should order NUMBER values correctly") {
    verifyOrderabilityReflexivity(FLOAT_valueGroups)
    verifyOrderabilityOrder(NUMBER_valueGroups)
  }

  private def verifyOrderabilityReflexivity[V <: CypherValue : Companion](values: ValueGroups[V]): Unit = {
    values.flatten.foreach { v =>
      orderability[V].compare(v, v) should be(0)
      if (! companion[V].isNull(v)) {
        (orderability[V].compare(v, cypherNull) < 0) should be(true)
        (orderability[V].compare(cypherNull, v) > 0) should be(true)
      }
    }

    (orderability[V].compare(cypherNull, cypherNull) == 0) should be(true)

    values.indexed.zip(values.indexed).foreach { entry =>
      val ((leftIndex, leftValue), (rightIndex, rightValue)) = entry
      val cmp = orderability[V].compare(leftValue, rightValue)
      val isEqual = cmp == 0
      val isSameValue = leftIndex == rightIndex
      isEqual should be(isSameValue)
    }
  }

  private def verifyOrderabilityOrder[V <: CypherValue : Companion](expected: ValueGroups[V]): Unit = {
    1.to(1000).foreach { _ =>
      val shuffled = Random.shuffle[Seq[V], Seq](expected)
      val sorted = shuffled.sortBy(values => Random.shuffle(values).head)(orderability)

      assertSameGroupsInSameOrder(sorted, expected)(orderability)
    }
  }

  @tailrec
  private def assertSameGroupsInSameOrder[V <: CypherValue](lhs: ValueGroups[V], rhs: ValueGroups[V])
                                                           (implicit order: Ordering[V]): Unit =
    (lhs, rhs) match {

      case (Seq(lefts, lhsTail@_*), Seq(rights, rhsTail@_*)) =>

        // each group only contains values that are indistinguishable under the order
        lefts.foreach { (l1: V) => lefts.foreach { (l2: V) => order.compare(l1, l2) should equal(0) } }
        rights.foreach { (r1: V) => rights.foreach { (r2: V) => order.compare(r1, r2) should equal(0) } }

        // each value in the left group comes before any value in the right group according to the order
        lefts.foreach { (l: V) => rights.foreach { (r: V) => order.compare(l, r) < 0 } }
        lefts.foreach { (l: V) => rights.foreach { (r: V) => order.compare(r, l) > 0 } }

        assertSameGroupsInSameOrder(lhsTail, rhsTail)

      case (Seq(), Seq()) =>
        // Yay! We win

      case _ =>
        fail("Value groups have differing length")
    }

  private def orderability[V <: CypherValue : Companion]: Ordering[V] = companion[V].orderability
}
