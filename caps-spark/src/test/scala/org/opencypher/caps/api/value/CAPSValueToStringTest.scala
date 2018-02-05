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

import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.test.CAPSTestSuite

class CypherValueToStringTest extends CAPSTestSuite {

  test("node") {
    CAPSNode(1L, Set.empty, CypherMap.empty).toCypherString should equal("()")
    CAPSNode(1L, Set("A"), CypherMap.empty).toCypherString should equal("(:A)")
    CAPSNode(1L, Set("A", "B"), CypherMap.empty).toCypherString should equal("(:A:B)")
    CAPSNode(1L, Set("A", "B"), CypherMap("a" -> "b")).toCypherString should equal("(:A:B {a: 'b'})")
    CAPSNode(1L, Set("A", "B"), CypherMap("a" -> "b", "b" -> 1)).toCypherString should equal("(:A:B {a: 'b', b: 1})")
    CAPSNode(1L, Set.empty, CypherMap("a" -> "b", "b" -> 1)).toCypherString should equal("({a: 'b', b: 1})")
  }

  test("relationship") {
    CAPSRelationship(1L, 1L, 1L, "A", CypherMap.empty).toCypherString should equal("[:A]")
    CAPSRelationship(1L, 1L, 1L, "A", CypherMap("a" -> "b")).toCypherString should equal("[:A {a: 'b'}]")
    CAPSRelationship(1L, 1L, 1L, "A", CypherMap("a" -> "b", "b" -> 1)).toCypherString should equal("[:A {a: 'b', b: 1}]")
  }

  test("literals") {
    CypherInteger(1L).toCypherString should equal("1")
    CypherFloat(3.14).toCypherString should equal("3.14")
    CypherString("foo").toCypherString should equal("'foo'")
    CypherString("").toCypherString should equal("''")
    CypherBoolean(true).toCypherString should equal("true")
  }

  test("list") {
    CypherList().toCypherString should equal("[]")
    CypherList("A", "B", 1L).toCypherString should equal("['A', 'B', 1]")
  }

  test("map") {
    CypherMap().toCypherString should equal("{}")
    CypherMap("a" -> 1).toCypherString should equal("{a: 1}")
    CypherMap("a" -> 1, "b" -> true).toCypherString should equal("{a: 1, b: true}")
  }
}
