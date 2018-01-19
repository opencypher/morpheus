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

import org.opencypher.caps.test.BaseTestSuite

class CypherValueToStringTest extends BaseTestSuite {

  test("node") {
    CypherNode(1L, Seq.empty, Properties.empty).toString should equal("()")
    CypherNode(1L, Seq("A"), Properties.empty).toString should equal("(:A)")
    CypherNode(1L, Seq("A", "B"), Properties.empty).toString should equal("(:A:B)")
    CypherNode(1L, Seq("A", "B"), Properties("a" -> "b")).toString should equal("(:A:B {a: 'b'})")
    CypherNode(1L, Seq("A", "B"), Properties("a" -> "b", "b" -> 1)).toString should equal("(:A:B {a: 'b', b: 1})")
    CypherNode(1L, Seq.empty, Properties("a" -> "b", "b" -> 1)).toString should equal("({a: 'b', b: 1})")
  }

  test("relationship") {
    CypherRelationship(1L, 1L, 1L, "A", Properties.empty).toString should equal("[:A]")
    CypherRelationship(1L, 1L, 1L, "A", Properties("a" -> "b")).toString should equal("[:A {a: 'b'}]")
    CypherRelationship(1L, 1L, 1L, "A", Properties("a" -> "b", "b" -> 1)).toString should equal("[:A {a: 'b', b: 1}]")
  }

  test("literals") {
    CypherInteger(1L).toString should equal("1")
    CypherFloat(3.14).toString should equal("3.14")
    CypherString("foo").toString should equal("'foo'")
    CypherString("").toString should equal("''")
    CypherBoolean(true).toString should equal("true")
  }

  test("list") {
    CypherList(Seq.empty).toString should equal("[]")
    CypherList(Seq("A", "B", 1L)).toString should equal("['A', 'B', 1]")
  }

  test("map") {
    CypherMap().toString should equal("{}")
    CypherMap("a" -> 1).toString should equal("{a: 1}")
    CypherMap("a" -> 1, "b" -> true).toString should equal("{a: 1, b: true}")
  }
}
