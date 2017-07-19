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
package org.opencypher.spark.api.value

import org.opencypher.spark.api.types.{Maybe, Ternary, True}
import org.opencypher.spark.api.value.CypherValue.companion

class CypherValueEqualTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("PATH equal") {
    verifyEqual(PATH_valueGroups)
  }

  test("RELATIONSHIP equal") {
    verifyEqual(RELATIONSHIP_valueGroups)
  }

  test("NODE equal") {
    verifyEqual(NODE_valueGroups)
  }

  test("MAP equal") {
    verifyEqual(MAP_valueGroups)
  }

  test("LIST equal") {
    verifyEqual(LIST_valueGroups)
  }

  test("BOOLEAN equal") {
    verifyEqual(BOOLEAN_valueGroups)
  }

  test("INTEGER equal") {
    verifyEqual(INTEGER_valueGroups)
  }

  test("FLOAT equal") {
    verifyEqual(FLOAT_valueGroups)
  }

  test("NUMBER equal") {
    verifyEqual(NUMBER_valueGroups)
  }

  test("ANY equal") {
    verifyEqual(ANY_valueGroups)
  }

  def verifyEqual[V <: CypherValue : CypherValueCompanion](valueGroups: ValueGroups[V]): Unit = {
    val values = valueGroups.flatten

    values.foreach { v => equal[V](v, v) should be(if (companion[V].isIncomparable(v)) Maybe else True) }
    values.foreach { v => equal[V](cypherNull, v) should be(Maybe) }
    values.foreach { v => equal[V](v, cypherNull) should be(Maybe) }

    equal[V](cypherNull, cypherNull) should be(Maybe)

    values.foreach { v1 =>
      values.foreach { v2 =>
        if (companion[V].isIncomparable(v1) || companion[V].isIncomparable(v2))
          equal[V](v1, v2) should be(Maybe)
        else {
          equal[V](v1, v2) should be(Ternary(v1 == v2))
        }
      }
    }
  }

  private def equal[V <: CypherValue : CypherValueCompanion](v1: V, v2: V): Ternary = {
    val cmp1 = companion[V].equal(v1, v2)
    val cmp2 = companion[V].equal(v2, v1)

    cmp1 should equal(cmp2)

    cmp1
  }
}
