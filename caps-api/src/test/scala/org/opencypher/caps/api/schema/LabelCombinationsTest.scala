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
package org.opencypher.caps.api.schema

import org.opencypher.caps.test.BaseTestSuite

class LabelCombinationsTest extends BaseTestSuite {

  test("combinationsFor") {
    val in = LabelCombinations(Set(
      Set("A"), Set("A", "B", "X"), Set("A", "X"), Set("B")
    ))

    in.combinationsFor(Set.empty) should equal(in.combos)
    in.combinationsFor(Set("A")) should equal(Set(
      Set("A"), Set("A", "B", "X"), Set("A", "X")
    ))
    in.combinationsFor(Set("B")) should equal(Set(
      Set("B"), Set("A", "B", "X")
    ))
    in.combinationsFor(Set("A", "X")) should equal(Set(
      Set("A", "B", "X"), Set("A", "X")
    ))
    in.combinationsFor(Set("A", "B")) should equal(Set(
      Set("A", "B", "X")
    ))
    in.combinationsFor(Set("A", "C")) shouldBe empty
  }

}
