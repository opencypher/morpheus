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
package org.opencypher.okapi.api.schema

import org.opencypher.okapi.api.types.{CTAny, CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.test.BaseTestSuite

class RelTypePropertyMapTest extends BaseTestSuite {

  test("++") {
    val map1 = RelTypePropertyMap.empty
      .register("A")("name" -> CTString, "age" -> CTInteger, "gender" -> CTString)
      .register("B")("p" -> CTBoolean)

    val map2 = RelTypePropertyMap.empty
      .register("A")("name" -> CTString, "gender" -> CTBoolean)
      .register("C")("name" -> CTString)

    map1 ++ map2 should equal(
      RelTypePropertyMap.empty
        .register("A")("name" -> CTString, "age" -> CTInteger, "gender" -> CTAny)
        .register("B")("p" -> CTBoolean)
        .register("C")("name" -> CTString)
    )
  }

  test("properties") {
    val map = RelTypePropertyMap.empty
      .register("A")("name" -> CTString)
      .register("B")("foo" -> CTInteger)

    map.properties("A") should equal(Map("name" -> CTString))
    map.properties("B") should equal(Map("foo" -> CTInteger))
    map.properties("C") should equal(Map.empty)
  }

  test("filter for rel types") {
    val map = RelTypePropertyMap.empty
      .register("A")("name" -> CTString)
      .register("B")("foo" -> CTInteger)

    map.filterForRelTypes(Set("A", "B")) should equal(RelTypePropertyMap.empty
      .register("A")("name" -> CTString)
      .register("B")("foo" -> CTInteger))
  }
}
