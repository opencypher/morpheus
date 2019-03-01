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

class TypeConverterTest extends BaseTestSuite {

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
    frontend.CTMap shouldBeConvertedTo CTMap(Map.empty)
  }

  implicit class RichType(t: frontend.CypherType) {
    def shouldBeConvertedTo(other: CypherType): Assertion = {
      TypeConverter.convert(t) should equal(Some(other))
    }
  }
}
