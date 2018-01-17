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
package org.opencypher.caps.ir.api.expr

import org.opencypher.caps.api.types.{CTNode, CTNull, CTRelationship, CTWildcard}
import org.scalatest.{FunSuite, Matchers}

class ExprTest extends FunSuite with Matchers {

  test("expressions ignore cypher type in equality") {
    val n = Var("a")(CTNode)
    val r = Var("a")(CTRelationship)
    n should equal(r)

    val a = StartNode(Var("rel")(CTRelationship))(CTWildcard)
    val b = StartNode(Var("rel")(CTRelationship))(CTNode)
    a should equal(b)
  }

  test("same expressions with different cypher types have the same hash code") {
    val n = Var("a")(CTNode("a"))
    val r = Var("a")(CTRelationship("b"))
    n.hashCode should equal(r.hashCode)

    val a = StartNode(Var("rel")(CTRelationship))(CTWildcard)
    val b = StartNode(Var("rel")(CTRelationship))(CTNode)
    a.hashCode should equal(b.hashCode)
  }

  test("different expressions are not equal") {
    val p = Param("a")()
    val v = Var("a")()
    p should not equal (v)
  }

  test("different expressions have different hash codes") {
    val p = Param("a")()
    val v = Var("b")()
    p.hashCode should not equal (v.hashCode)
  }

}
