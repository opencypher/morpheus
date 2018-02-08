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
package org.opencypher.caps.ir.impl.typer

import org.neo4j.cypher.internal.util.v3_4.{symbols => frontend}
import org.opencypher.caps.api.types._
import org.opencypher.caps.test.BaseTestSuite
import org.scalatest.Assertion

class fromFrontendTypeTest extends BaseTestSuite {

  test("should convert basic types") {
    frontend.CTBoolean shouldBeConvertedTo CTBoolean
    frontend.CTInteger shouldBeConvertedTo CTInteger
    frontend.CTFloat shouldBeConvertedTo CTFloat
    frontend.CTNumber shouldBeConvertedTo CTNumber
    frontend.CTString shouldBeConvertedTo CTString
    frontend.CTAny shouldBeConvertedTo CTAny
  }

  test("should convert entity types") {
    frontend.CTNode shouldBeConvertedTo CTNode
    frontend.CTRelationship shouldBeConvertedTo CTRelationship
    frontend.CTPath shouldBeConvertedTo CTPath
  }

  test("should convert container types") {
    frontend.CTList(frontend.CTInteger) shouldBeConvertedTo CTList(CTInteger)
    frontend.CTMap shouldBeConvertedTo CTMap
  }

  implicit class RichType(t: frontend.CypherType) {
    def shouldBeConvertedTo(other: CypherType): Assertion = {
      fromFrontendType(t) should equal(other)
    }
  }
}
