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
package org.opencypher.okapi.impl.util

import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.impl.exception.IllegalArgumentException

class VersionTest extends ApiBaseTest {
  describe("parsing") {
    it("parses two valued version numbers") {
      Version("1.0") should equal(Version(1,0))
      Version("1.5") should equal(Version(1,5))
      Version("42.21") should equal(Version(42,21))
    }

    it("parses single valued version numbers") {
      Version("1") should equal(Version(1,0))
      Version("42") should equal(Version(42,0))
    }

    it("throws errors on malformed version string") {
      an[IllegalArgumentException] shouldBe thrownBy(Version("foo"))
      an[IllegalArgumentException] shouldBe thrownBy(Version("1.foo"))
      an[IllegalArgumentException] shouldBe thrownBy(Version("foo.bar"))
    }
  }

  describe("toString") {
    it("turns into a version string") {
      Version("1.5").toString shouldEqual "1.5"
      Version("2").toString shouldEqual "2.0"
    }
  }

  describe("compatibleWith") {
    it("is compatible with it self") {
      Version("1.5").compatibleWith(Version("1.5")) shouldBe true
    }

    it("is compatible if the major versions match") {
      Version("1.5").compatibleWith(Version("1.0")) shouldBe true
      Version("1.0").compatibleWith(Version("1.5")) shouldBe true
    }

    it("is incompatible if the major versions differ") {
      Version("1.5").compatibleWith(Version("2.5")) shouldBe false
      Version("2.0").compatibleWith(Version("1.5")) shouldBe false
    }
  }
}
