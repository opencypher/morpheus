/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.value

import scala.annotation.tailrec
import scala.util.Random

class CypherValueOrderabilityTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("should order PATH values correctly") {
    verifyOrderabilityReflexivity(PATH_valueGroups)
    verifyOrderabilityTransitivity(PATH_valueGroups)
    verifyOrderabilityOrder(PATH_valueGroups)
  }

  test("should order RELATIONSHIP values correctly") {
    verifyOrderabilityReflexivity(RELATIONSHIP_valueGroups)
    verifyOrderabilityTransitivity(RELATIONSHIP_valueGroups)
    verifyOrderabilityOrder(RELATIONSHIP_valueGroups)
  }

  test("should order NODE values correctly") {
    verifyOrderabilityReflexivity(NODE_valueGroups)
    verifyOrderabilityTransitivity(NODE_valueGroups)
    verifyOrderabilityOrder(NODE_valueGroups)
  }

  test("should order MAP values correctly") {
    verifyOrderabilityReflexivity(MAP_valueGroups)
    verifyOrderabilityTransitivity(MAP_valueGroups)
    verifyOrderabilityOrder(MAP_valueGroups)
  }

  test("should order LIST values correctly") {
    verifyOrderabilityReflexivity(LIST_valueGroups)
    verifyOrderabilityTransitivity(LIST_valueGroups)
    verifyOrderabilityOrder(LIST_valueGroups)
  }

  test("should order STRING values correctly") {
    verifyOrderabilityReflexivity(STRING_valueGroups)
    verifyOrderabilityTransitivity(STRING_valueGroups)
    verifyOrderabilityOrder(STRING_valueGroups)
  }

  test("should order BOOLEAN values correctly") {
    verifyOrderabilityReflexivity(BOOLEAN_valueGroups)
    verifyOrderabilityTransitivity(BOOLEAN_valueGroups)
    verifyOrderabilityOrder(BOOLEAN_valueGroups)
  }

  test("should order INTEGER values correctly") {
    verifyOrderabilityReflexivity(INTEGER_valueGroups)
    verifyOrderabilityTransitivity(INTEGER_valueGroups)
    verifyOrderabilityOrder(INTEGER_valueGroups)
  }

  test("should order FLOAT values correctly") {
    verifyOrderabilityReflexivity(FLOAT_valueGroups)
    verifyOrderabilityTransitivity(FLOAT_valueGroups)
    verifyOrderabilityOrder(FLOAT_valueGroups)
  }

  test("should order NUMBER values correctly") {
    verifyOrderabilityReflexivity(NUMBER_valueGroups)
    verifyOrderabilityTransitivity(NUMBER_valueGroups)
    verifyOrderabilityOrder(NUMBER_valueGroups)
  }

  test("should order ANY values correctly") {
    verifyOrderabilityReflexivity(ANY_valueGroups)
    verifyOrderabilityTransitivity(ANY_valueGroups)
    verifyOrderabilityOrder(ANY_valueGroups)
  }

  private def verifyOrderabilityReflexivity[V <: CypherValue : CypherValueCompanion](values: ValueGroups[V]): Unit = {
    values.flatten.foreach { (v: V) =>
      CypherValueCompanion[V].order.compare(v, v) should be(0)
      if (! v.isNull) {
        (CypherValueCompanion[V].order.compare(v, cypherNull[V]) < 0) should be(true)
        (CypherValueCompanion[V].order.compare(cypherNull[V], v) > 0) should be(true)
      }
    }

    (CypherValueCompanion[V].order.compare(cypherNull[V], cypherNull[V]) == 0) should be(true)

    values.indexed.zip(values.indexed).foreach { entry =>
      val ((leftIndex, leftValue), (rightIndex, rightValue)) = entry
      val cmp = CypherValueCompanion[V].order.compare(leftValue, rightValue)
      val isEqual = cmp == 0
      val isSameValue = leftIndex == rightIndex
      isEqual should be(isSameValue)
    }
  }

  private def verifyOrderabilityTransitivity[V <: CypherValue : CypherValueCompanion](values: ValueGroups[V]): Unit = {
    var count = 0
    val flatValues = values.indexed
    flatValues.foreach { a =>
      flatValues.foreach { b =>
        flatValues.foreach { c =>
          val (i1, v1) = a
          val (i2, v2) = b
          val (i3, v3) = c

          val cmp1 = CypherValueCompanion[V].order(v1, v2)
          val cmp2 = CypherValueCompanion[V].order(v2, v3)
          val cmp3 = CypherValueCompanion[V].order(v1, v3)

//          println(s"$count $a << $cmp1 >> $b << $cmp2 >> $c : $cmp3")
//          count += 1

          assertInIndexOrder(cmp1)(i1, i2)
          assertInIndexOrder(cmp2)(i2, i3)
          assertInIndexOrder(cmp3)(i1, i3)

          if (cmp1 == 0 && cmp2 == 0)
            (cmp3 == 0) should be(true)

          if (cmp1 < 0 && cmp2 < 0)
            (cmp3 < 0) should be(true)

          if (cmp1 == 0 && cmp2 < 0)
            (cmp3 < 0) should be(true)

          if (cmp1 < 0 && cmp2 == 0)
            (cmp3 < 0) should be(true)

          if (cmp1 > 0 && cmp2 > 0)
            (cmp3 > 0) should be(true)

          if (cmp1 == 0 && cmp2 > 0)
            (cmp3 > 0) should be(true)

          if (cmp1 > 0 && cmp2 == 0)
            (cmp3 > 0) should be(true)
        }
      }
    }
  }

  private def assertInIndexOrder(cmp: Int)(leftIndex: Int, rightIndex: Int) = {
    if (cmp < 0) (leftIndex < rightIndex)  should be(true)
    if (cmp == 0) (leftIndex == rightIndex)should be(true)
    if (cmp > 0) (leftIndex > rightIndex) should be(true)
  }

  private def verifyOrderabilityOrder[V <: CypherValue : CypherValueCompanion](expected: ValueGroups[V]): Unit = {
    1.to(1000).foreach { _ =>
      val shuffled = Random.shuffle[Seq[V], Seq](expected)
      val sorted = shuffled.sortBy(values => Random.shuffle(values).head)(CypherValueCompanion[V].order)

      assertSameGroupsInSameOrder(sorted, expected)(CypherValueCompanion[V].order)
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
}
