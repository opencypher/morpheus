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
package org.opencypher.spark.impl.acceptance

import org.junit.runner.RunWith
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.{Bag, TestNameFixture}
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NullTests extends CAPSTestSuite with ScanGraphInit with TestNameFixture {

  override protected def separator: String = "calling:"

  private def returnsNull(call: String = testName) =
    returnsValue(null, call)

  private def returnsValue(exp: Any, call: String = testName) =
    caps
      .cypher(s"RETURN $call AS res")
      .records.toMaps
      .shouldEqual(Bag(CypherMap("res" -> exp)))


  describe("null input produces null") {
    it("calling: id(null)")(returnsNull())
    it("calling: labels(null)")(returnsNull())
    it("calling: type(null)")(returnsNull())
    it("calling: exists(null.b)")(returnsNull())
    it("calling: size(null)")(returnsNull())
    it("calling: keys(null)")(returnsNull())
    it("calling: startNode(null)")(returnsNull())
    it("calling: endNode(null)")(returnsNull())
    it("calling: toFloat(null)")(returnsNull())
    it("calling: toInteger(null)")(returnsNull())
    it("calling: toString(null)")(returnsNull())
    it("calling: toBoolean(null)")(returnsNull())
    it("calling: trim(null)")(returnsNull())
    it("calling: lTrim(null)")(returnsNull())
    it("calling: rTrim(null)")(returnsNull())
    it("calling: toUpper(null)")(returnsNull())
    it("calling: toLower(null)")(returnsNull())
    it("calling: properties(null)")(returnsNull())
    it("calling: sqrt(null)")(returnsNull())
    it("calling: log(null)")(returnsNull())
    it("calling: log10(null)")(returnsNull())
    it("calling: exp(null)")(returnsNull())
    it("calling: abs(null)")(returnsNull())
    it("calling: ceil(null)")(returnsNull())
    it("calling: floor(null)")(returnsNull())
    it("calling: round(null)")(returnsNull())
    it("calling: sign(null)")(returnsNull())
    it("calling: acos(null)")(returnsNull())
    it("calling: asin(null)")(returnsNull())
    it("calling: atan(null)")(returnsNull())
    it("calling: cos(null)")(returnsNull())
    it("calling: cot(null)")(returnsNull())
    it("calling: degrees(null)")(returnsNull())
    it("calling: haversin(null)")(returnsNull())
    it("calling: radians(null)")(returnsNull())
    it("calling: sin(null)")(returnsNull())
    it("calling: tan(null)")(returnsNull())
    it("calling: null STARTS WITH null")(returnsNull())
    it("calling: null ENDS WITH null")(returnsNull())
    it("calling: null CONTAINS null")(returnsNull())
    it("calling: null = null")(returnsNull())
    it("calling: null =~ null")(returnsNull())
    it("calling: null < null")(returnsNull())
    it("calling: null <= null")(returnsNull())
    it("calling: null > null")(returnsNull())
    it("calling: null >= null")(returnsNull())
    it("calling: null IN null")(returnsNull())
    it("calling: NOT null")(returnsNull())
    it("calling: null:FOO")(returnsNull())
    it("calling: type(null) = null")(returnsNull())
    it("calling: null + null")(returnsNull())
    it("calling: null - null")(returnsNull())
    it("calling: null * null")(returnsNull())
    it("calling: null / null")(returnsNull())
    it("calling: null.foo")(returnsNull())
    it("calling: range(null, null)")(returnsNull())
    it("calling: replace(null, null, null)")(returnsNull())
    it("calling: substring(null, null, null)")(returnsNull())
    it("calling: atan2(null, null)")(returnsNull())
    it("calling: avg(null)")(returnsNull())
    it("calling: max(null)")(returnsNull())
    it("calling: min(null)")(returnsNull())
    it("calling: sum(null)")(returnsNull())
  }

  describe("null input produces value") {
    it("calling: null IS NULL")(returnsValue(true))
    it("calling: null IS NOT NULL")(returnsValue(false))
    it("calling: count(null)")(returnsValue(0))
    it("calling: collect(null)")(returnsValue(List()))
  }

}
