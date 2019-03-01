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
package org.opencypher.okapi.ir.impl.typer

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.testing.BaseTestSuite
import org.neo4j.cypher.internal.v4_0.util.{symbols => frontend}
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
    CTMap(Map.empty) shouldBeConvertedTo frontend.CTMap
    CTMap(Map.empty).nullable shouldBeConvertedTo frontend.CTMap
  }

  implicit class RichType(t: CypherType) {
    def shouldBeConvertedTo(other: frontend.CypherType): Assertion = {
      toFrontendType(t) should equal(other)
    }
  }
}
