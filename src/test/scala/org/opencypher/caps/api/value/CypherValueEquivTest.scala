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
      if (! v.isNull) {
        (v `equivTo` cypherNull[V]) should be(false)
        (cypherNull[V] `equivTo` v) should be(false)
      }
    }

    (cypherNull[V] `equivTo` cypherNull[V]) should be(true)

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
    val b1 = CypherValueCompanion[V].equiv(v1, v2)
    val b2 = CypherValueCompanion[V].equiv(v2, v1)

//    println(s"$v1 $v2 $b1 $b2")

    b1 should be(b2)
    (v1 == v2) should be(b2)
    (v2 == v1) should be(b2)

    b1
  }
}
