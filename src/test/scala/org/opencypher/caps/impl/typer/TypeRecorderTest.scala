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
package org.opencypher.caps.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.Ref
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, True}
import org.opencypher.caps.BaseTestSuite
import org.opencypher.caps.api.types.{CTBoolean, CTString}

class TypeRecorderTest extends BaseTestSuite with AstConstructionTestSupport {

  test("can convert to map") {
    val expr1 = True()(pos)
    val expr2 = True()(pos)
    val recorder = TypeRecorder(List(Ref(expr1) -> CTBoolean, Ref(expr2) -> CTString))

    recorder.toMap should equal(Map(Ref(expr1) -> CTBoolean, Ref(expr2) -> CTString))
  }

}
