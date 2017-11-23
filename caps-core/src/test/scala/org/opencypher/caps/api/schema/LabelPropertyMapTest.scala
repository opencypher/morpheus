/*
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
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.caps.test.BaseTestSuite

class LabelPropertyMapTest extends BaseTestSuite {

  test("++") {
    val map1 = LabelPropertyMap.empty
      .register("A")("name" -> CTString)
      .register("B")("p" -> CTBoolean)

    val map2 = LabelPropertyMap.empty
      .register("A")("name" -> CTString)
      .register("C")("name" -> CTString)

    map1 ++ map2 should equal(
      LabelPropertyMap.empty
        .register("A")("name" -> CTString)
        .register("B")("p" -> CTBoolean)
        .register("C")("name" -> CTString)
    )
  }

  test("for labels") {
    val map = LabelPropertyMap.empty
      .register("A")("name" -> CTString)
      .register("B")("foo" -> CTInteger)
      .register("B", "A")("foo" -> CTInteger)
      .register("C")("bar" -> CTInteger)

    map.forLabels("A") should equal(LabelPropertyMap.empty
      .register("A")("name" -> CTString)
      .register("A", "B")("foo" -> CTInteger)
    )
    map.forLabels("C") should equal(LabelPropertyMap.empty
      .register("C")("bar" -> CTInteger)
    )
    map.forLabels("X") should equal(LabelPropertyMap.empty)
    map.forLabels("A", "B", "C") should equal(map)
  }

}
