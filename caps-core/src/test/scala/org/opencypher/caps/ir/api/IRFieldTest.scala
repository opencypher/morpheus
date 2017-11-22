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
package org.opencypher.caps.ir.api

import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.scalatest.{FunSuite, Matchers}

class IRFieldTest extends FunSuite with Matchers {

  test("IRField ignores cypher type in equality") {
    val a = IRField("a")(CTNode)
    val b = IRField("a")(CTRelationship)
    a should equal(b)
  }

  test("same IRFields with different cypher types have the same hash code") {
    val n = IRField("a")(CTNode("a"))
    val r = IRField("a")(CTRelationship("b"))
    n.hashCode should equal(r.hashCode)
  }

  test("different IRFields are not equal") {
    val a = IRField("a")()
    val b = IRField("b")()
    a should not equal(b)
  }

  test("different IRFields have different hash codes") {
    val a = IRField("a")()
    val b = IRField("b")()
    a.hashCode should not equal(b.hashCode)
  }

}
