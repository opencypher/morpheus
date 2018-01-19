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
package org.opencypher.caps.api.types

import org.opencypher.caps.test.BaseTestSuite

class TernaryTest extends BaseTestSuite {

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
