/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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

class CypherValueComparabilityTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("should compare PATH values correctly") {
    verifyComparability(PATH_valueGroups)
  }

  test("should compare RELATIONSHIP values correctly") {
    verifyComparability(RELATIONSHIP_valueGroups)
  }

  test("should compare NODE values correctly") {
    verifyComparability(NODE_valueGroups)
  }

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

  private def verifyComparability[V <: CypherValue : CypherValueCompanion](valueGroups: ValueGroups[V]): Unit = {
    valueGroups.flatten.foreach { v =>
      tryCompare(v, v) should be(if (v.comparesNulls) None else Some(0))
    }

    val indexedValueGroups =
      valueGroups
        .zipWithIndex
        .flatMap { case ((group), index) => group.map { v => index -> v } }

    indexedValueGroups.foreach { left =>
      val ((leftIndex, leftValue)) = left
      indexedValueGroups.foreach { right =>
        val ((rightIndex, rightValue)) = right
        val cmp = tryCompare(leftValue, rightValue)

        // direction 1: knowing we have the same value
        val isSameValue = leftIndex == rightIndex
        if (isSameValue)
          cmp should equal(if (leftValue.comparesNulls || rightValue.comparesNulls) None else Some(0))
        else
          cmp should not equal Some(0)

        // direction 2: knowing the values are comparable
        if (cmp.nonEmpty && cmp.get <= 0) {
          (leftIndex <= rightIndex) should be(true)
          (CypherValueCompanion[V].order(leftValue, rightValue) <= 0) should be(true)
        }
      }
    }
  }

  private def tryCompare[V <: CypherValue](l: V, r: V)(implicit companion: CypherValueCompanion[V]): Option[Int] = {
    val a = companion.compare(l, r)
    val b = companion.compare(r, l)

    (a, b) match {
      case (Some(0), Some(0)) =>
        Some(0)

      case (Some(x), Some(y)) =>
        ((x < 0 && y > 0) || (x > 0 && y < 0)) should equal(true)
        Some(x)

      case (None, None) =>
        None

      case _ =>
        fail("Comparability symmetry is broken")
    }
  }
}
