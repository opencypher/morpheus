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

import org.opencypher.caps.api.value.CypherValue.{CypherMap, _}
import org.opencypher.caps.test.CAPSTestSuite

class CAPSValueToStringTest extends CAPSTestSuite {

  test("node") {
    CAPSNode(1L, Set.empty, Properties.empty).toString should equal("()")
    CAPSNode(1L, Set("A"), Properties.empty).toString should equal("(:A)")
    CAPSNode(1L, Set("A", "B"), Properties.empty).toString should equal("(:A:B)")
    CAPSNode(1L, Set("A", "B"), Properties("a" -> "b")).toString should equal("(:A:B {a: 'b'})")
    CAPSNode(1L, Set("A", "B"), Properties("a" -> "b", "b" -> 1)).toString should equal("(:A:B {a: 'b', b: 1})")
    CAPSNode(1L, Set.empty, Properties("a" -> "b", "b" -> 1)).toString should equal("({a: 'b', b: 1})")
  }

  test("relationship") {
    CAPSRelationship(1L, 1L, 1L, "A", Properties.empty).toString should equal("[:A]")
    CAPSRelationship(1L, 1L, 1L, "A", Properties("a" -> "b")).toString should equal("[:A {a: 'b'}]")
    CAPSRelationship(1L, 1L, 1L, "A", Properties("a" -> "b", "b" -> 1)).toString should equal("[:A {a: 'b', b: 1}]")
  }

  test("literals") {
    CypherInteger(1L).toString should equal("1")
    CypherFloat(3.14).toString should equal("3.14")
    CypherString("foo").toString should equal("'foo'")
    CypherString("").toString should equal("''")
    CypherBoolean(true).toString should equal("true")
  }

  test("list") {
    CypherList(List.empty[MaterialCypherValue]).toString should equal("[]")
    CypherList(List[MaterialCypherValue]("A", "B", 1L)).toString should equal("['A', 'B', 1]")
  }

  test("map") {
    CypherMap().toString should equal("{}")
    CypherMap("a" -> 1).toString should equal("{a: 1}")
    CypherMap("a" -> 1, "b" -> true).toString should equal("{a: 1, b: true}")
  }
}
