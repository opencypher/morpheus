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
package org.opencypher.okapi.ir.impl.typer

import org.neo4j.cypher.internal.util.v3_4.{symbols => frontend}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.test.BaseTestSuite
import org.scalatest.Assertion

class toFrontendTypeTest extends BaseTestSuite {

  it("can convert basic types") {
    CTAny shouldBeConvertedTo frontend.CTAny
    CTAny.nullable shouldBeConvertedTo frontend.CTAny
    CTInteger shouldBeConvertedTo frontend.CTInteger
    CTInteger.nullable shouldBeConvertedTo frontend.CTInteger
    CTFloat shouldBeConvertedTo frontend.CTFloat
    CTFloat.nullable shouldBeConvertedTo frontend.CTFloat
    CTNumber shouldBeConvertedTo frontend.CTNumber
    CTNumber.nullable shouldBeConvertedTo frontend.CTNumber
    CTBoolean shouldBeConvertedTo frontend.CTBoolean
    CTBoolean.nullable shouldBeConvertedTo frontend.CTBoolean
    CTString shouldBeConvertedTo frontend.CTString
    CTString.nullable shouldBeConvertedTo frontend.CTString
  }

  test("should convert entity types") {
    CTNode shouldBeConvertedTo frontend.CTNode
    CTNode.nullable shouldBeConvertedTo frontend.CTNode
    CTRelationship shouldBeConvertedTo frontend.CTRelationship
    CTRelationship.nullable shouldBeConvertedTo frontend.CTRelationship
    CTPath shouldBeConvertedTo frontend.CTPath
  }

  test("should convert container types") {
    CTList(CTInteger) shouldBeConvertedTo frontend.CTList(frontend.CTInteger)
    CTList(CTInteger).nullable shouldBeConvertedTo frontend.CTList(frontend.CTInteger)
    CTList(CTInteger.nullable) shouldBeConvertedTo frontend.CTList(frontend.CTInteger)
    CTMap shouldBeConvertedTo frontend.CTMap
    CTMap.nullable shouldBeConvertedTo frontend.CTMap
  }

  implicit class RichType(t: CypherType) {
    def shouldBeConvertedTo(other: frontend.CypherType): Assertion = {
      toFrontendType(t) should equal(other)
    }
  }
}
