/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.api.pattern

import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.v9_0.expressions.SemanticDirection.{INCOMING, OUTGOING}

class ConnectionTest extends BaseTestSuite {

  val field_a: IRField = IRField("a")()
  val field_b: IRField = IRField("b")()
  val field_c: IRField = IRField("c")()

  val relType = CTRelationship("FOO")

  test("SimpleConnection.flip") {
    DirectedRelationship(field_a, field_b).flip should equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a).flip should equal(DirectedRelationship(field_a, field_a))
  }

  test("SimpleConnection.flip with semantic direction") {
    DirectedRelationship(field_a, field_b, OUTGOING).flip should equal(DirectedRelationship(field_b, field_a, INCOMING))
    DirectedRelationship(field_a, field_a, INCOMING).flip should equal(DirectedRelationship(field_a, field_a, OUTGOING))
  }

  test("SimpleConnection.equals") {
    DirectedRelationship(field_a, field_b) shouldNot equal(DirectedRelationship(field_b, field_a))
    DirectedRelationship(field_a, field_a) should equal(DirectedRelationship(field_a, field_a))
    DirectedRelationship(field_a, field_a, OUTGOING) should equal(DirectedRelationship(field_a, field_a, OUTGOING))
    DirectedRelationship(field_a, field_a) shouldNot equal(DirectedRelationship(field_a, field_b))
  }

  test("VarLengthRelationship.flip") {
    DirectedVarLengthRelationship(relType, field_a -> field_b, 0, Some(0)).flip should equal(
      DirectedVarLengthRelationship(relType, field_b -> field_a, 0, Some(0)))

    DirectedVarLengthRelationship(relType, field_a -> field_a, 0, Some(0)).flip should equal(
      DirectedVarLengthRelationship(relType, field_a -> field_a, 0, Some(0)))
  }

  test("VarLengthRelationship.flip with semantic direction") {
    DirectedVarLengthRelationship(relType, field_a -> field_b, 0, Some(0), OUTGOING).flip should equal(
      DirectedVarLengthRelationship(relType, field_b -> field_a, 0, Some(0), INCOMING))

    DirectedVarLengthRelationship(relType, field_a -> field_a, 0, Some(0), INCOMING).flip should equal(
      DirectedVarLengthRelationship(relType, field_a -> field_a, 0, Some(0), OUTGOING))
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
