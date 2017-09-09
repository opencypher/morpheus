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
package org.opencypher.caps.ir.api.pattern

import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.ir.impl.IrTestSuite
import org.opencypher.caps.toField

class PatternTest extends IrTestSuite {

  test("add connection") {
    Pattern.empty[Expr]
      .withConnection('r, DirectedRelationship('a, 'b)) should equal(
      Pattern(Map.empty, Map(toField('r) -> DirectedRelationship('a, 'b)))
    )
  }

  test("mark node as solved") {
    Pattern.empty[Expr]
      .withEntity('a, EveryNode)
      .withEntity('b, EveryNode)
      .withEntity('r, EveryRelationship)
      .solvedNode('a) should equal(
        Pattern(Map(toField('b) -> EveryNode, toField('r) -> EveryRelationship), Map.empty)
    )
  }

  test("mark connection as solved") {
    Pattern.empty[Expr]
      .withEntity('a, EveryNode)
      .withEntity('b, EveryNode)
      .withEntity('r, EveryRelationship)
      .withConnection('r, DirectedRelationship('a, 'b))
      .withoutConnection('r) should equal(
        Pattern(Map.empty, Map.empty)
    )
  }
}
