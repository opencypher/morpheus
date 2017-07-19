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
package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, False, True}
import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTString}

class TypeTrackerTest extends BaseTestSuite with AstConstructionTestSupport {

  test("insert and lookup") {
    val tracker = TypeTracker.empty.updated(True()(pos), CTString)

    tracker.get(True()(pos)) shouldBe Some(CTString)
  }

  test("push scope and lookup") {
    val tracker = TypeTracker.empty.updated(True()(pos), CTString).pushScope()

    tracker.get(True()(pos)) shouldBe Some(CTString)
  }

  test("pushing and popping scope") {
    val tracker1 = TypeTracker.empty.updated(True()(pos), CTString)

    val tracker2 = tracker1.pushScope().updated(False()(pos), CTBoolean).popScope()

    tracker1 should equal(tracker2.get)
  }

}
