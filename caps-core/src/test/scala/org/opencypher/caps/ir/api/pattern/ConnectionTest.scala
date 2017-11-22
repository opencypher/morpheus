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
package org.opencypher.caps.ir.api.pattern

import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.test.BaseTestSuite

class ConnectionTest extends BaseTestSuite {

  val field_a = IRField("a")()
  val field_b = IRField("b")()
  val field_c = IRField("c")()

  test("SimpleConnection.flip") {
    DirectedRelationship(field_a, field_b).flip should equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a).flip should equal(DirectedRelationship(field_a, field_a))
  }

  test("SimpleConnection.equals") {
    DirectedRelationship(field_a, field_b) shouldNot equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a) should equal(DirectedRelationship(field_a, field_a))
    DirectedRelationship(field_a, field_a) shouldNot equal(DirectedRelationship(field_a, field_b))
  }

  test("UndirectedConnection.flip") {
    UndirectedRelationship(field_a, field_b).flip should equal(UndirectedRelationship(field_b, field_a))
  }

  test("UndirectedConnection.equals") {
    UndirectedRelationship(field_a, field_b) should equal(UndirectedRelationship(field_b, field_a))
    UndirectedRelationship(field_c, field_c) should equal(UndirectedRelationship(field_c, field_c))
  }

  test("Mixed equals") {
    DirectedRelationship(field_a, field_a) should equal(UndirectedRelationship(field_a, field_a))
  }
}
