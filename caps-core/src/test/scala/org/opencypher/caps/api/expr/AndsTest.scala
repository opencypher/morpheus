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
package org.opencypher.caps.api.expr

import org.opencypher.caps.ir.api.Label
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.api.expr.ExprHelper._
import org.opencypher.caps.api.types.CypherType

class AndsTest extends BaseTestSuite {

  test("unnests inner ands") {
    val x = Var("x")()
    val args: Set[Expr] = Set(Ands(TrueLit()), HasLabel(x, Label("X"))(), Ands(Ands(Ands(FalseLit()))))

    Ands(args) should equal(Ands(TrueLit(), HasLabel(x, Label("X"))(), FalseLit()))
  }

  test("empty ands not allowed") {
    a [IllegalStateException] should be thrownBy {
      Ands()
    }
  }
}
