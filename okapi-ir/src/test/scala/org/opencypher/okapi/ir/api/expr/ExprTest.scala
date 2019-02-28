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
package org.opencypher.okapi.ir.api.expr

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.testing.BaseTestSuite

class ExprTest extends BaseTestSuite {

  test("expressions ignore cypher type in equality") {
    val n = Var("a")(CTInteger)
    val r = Var("a")(CTString)
    n should equal(r)

    val a = StartNode(Var("rel")(CTRelationship))(CTAny)
    val b = StartNode(Var("rel")(CTRelationship))(CTNode)
    a should equal(b)
  }

  test("same expressions with different cypher types have the same hash code") {
    val n = Var("a")(CTNode("a"))
    val r = Var("a")(CTRelationship("b"))
    n.hashCode should equal(r.hashCode)

    val a = StartNode(Var("rel")(CTRelationship))(CTAny)
    val b = StartNode(Var("rel")(CTRelationship))(CTNode)
    a.hashCode should equal(b.hashCode)
  }

  test("different expressions are not equal") {
    val p = Param("a")(CTAny)
    val v = Var("a")(CTAny)
    p should not equal v
  }

  test("different expressions have different hash codes") {
    val p = Param("a")(CTAny)
    val v = Var("b")(CTAny)
    p.hashCode should not equal v.hashCode
  }

  test("alias expression has updated type") {
    val n = Var("n")(CTNode)
    val aliasVar = Var("m")(CTAny)
    (n as aliasVar).cypherType should equal(aliasVar.cypherType)
  }

}
