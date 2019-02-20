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
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NullTests extends CAPSTestSuite with ScanGraphInit {

  private def returnsNull(call: String) =
    returnsSingle(call, null)

  private def returnsSingle(call: String, exp: Any) =
    caps
      .cypher(s"RETURN $call AS res")
      .records.toMaps
      .shouldEqual(Bag(CypherMap("res" -> exp)))

  describe("null input produces null") {
    it("id(null)")(returnsNull("id(null)"))
    it("prefixId(null)")(returnsNull("prefixId(null)")) // no syntax?
    it("toId(null)")(returnsNull("toId(null)")) // no syntax?
    it("labels(null)")(returnsNull("labels(null)")) // no inference
    it("type(null)")(returnsNull("type(null)")) // no inference
    it("exists(null)")(returnsNull("exists(null)")) // no inference
    it("size(null)")(returnsNull("size(null)"))
    it("keys(null)")(returnsNull("keys(null)"))
    it("startNodeFunction(null)")(returnsNull("startNodeFunction(null)"))
    it("endNodeFunction(null)")(returnsNull("endNodeFunction(null)"))
    it("toFloat(null)")(returnsNull("toFloat(null)"))
    it("toInteger(null)")(returnsNull("toInteger(null)"))
    it("toString(null)")(returnsNull("toString(null)"))
    it("toBoolean(null)")(returnsNull("toBoolean(null)"))
    it("explode(null)")(returnsNull("explode(null)"))
    it("trim(null)")(returnsNull("trim(null)"))
    it("lTrim(null)")(returnsNull("lTrim(null)"))
    it("rTrim(null)")(returnsNull("rTrim(null)"))
    it("toUpper(null)")(returnsNull("toUpper(null)"))
    it("toLower(null)")(returnsNull("toLower(null)"))
    it("properties(null)")(returnsNull("properties(null)"))
    it("sqrt(null)")(returnsNull("sqrt(null)"))
    it("log(null)")(returnsNull("log(null)"))
    it("log10(null)")(returnsNull("log10(null)"))
    it("exp(null)")(returnsNull("exp(null)"))
    it("abs(null)")(returnsNull("abs(null)"))
    it("ceil(null)")(returnsNull("ceil(null)"))
    it("floor(null)")(returnsNull("floor(null)"))
    it("round(null)")(returnsNull("round(null)"))
    it("sign(null)")(returnsNull("sign(null)"))
    it("acos(null)")(returnsNull("acos(null)"))
    it("asin(null)")(returnsNull("asin(null)"))
    it("atan(null)")(returnsNull("atan(null)"))
    it("cos(null)")(returnsNull("cos(null)"))
    it("cot(null)")(returnsNull("cot(null)"))
    it("degrees(null)")(returnsNull("degrees(null)"))
    it("haversin(null)")(returnsNull("haversin(null)"))
    it("radians(null)")(returnsNull("radians(null)"))
    it("sin(null)")(returnsNull("sin(null)"))
    it("tan(null)")(returnsNull("tan(null)"))
    it("null STARTS WITH null")(returnsNull("null STARTS WITH null"))
    it("null ENDS WITH null")(returnsNull("null ENDS WITH null"))
    it("null CONTAINS null")(returnsNull("null CONTAINS null"))
    it("null = null")(returnsNull("null = null"))
    it("null =~ null")(returnsNull("null =~ null"))
    it("null < null")(returnsNull("null < null"))
    it("null <= null")(returnsNull("null <= null"))
    it("null > null")(returnsNull("null > null"))
    it("null >= null")(returnsNull("null >= null"))
    it("null IN null")(returnsNull("null IN null"))
    it("null & null")(returnsNull("null & null"))
    it("null | null")(returnsNull("null | null"))
    it("null << null")(returnsNull("null << null"))
    it("null >>> null")(returnsNull("null >>> null"))
    it("NOT null")(returnsNull("NOT null"))
    it("null:FOO")(returnsNull("null:FOO"))
    it("type(null) = null")(returnsNull("type(null) = null"))
    it("null + null")(returnsNull("null + null"))
    it("null - null")(returnsNull("null - null"))
    it("null * null")(returnsNull("null * null"))
    it("null / null")(returnsNull("null / null"))
    it("null.foo")(returnsNull("null.foo"))
    it("range(null, null)")(returnsNull("range(null, null)"))
    it("replace(null, null, null)")(returnsNull("replace(null, null, null)"))
    it("substring(null, null, null)")(returnsNull("substring(null, null, null)"))
    it("atan2(null, null)")(returnsNull("atan2(null, null)"))
    it("avg(null)")(returnsNull("avg(null)"))
    it("max(null)")(returnsNull("max(null)"))
    it("min(null)")(returnsNull("min(null)"))
    it("sum(null)")(returnsNull("sum(null)"))
  }

  describe("null input produces value") {
    it("null IS NULL")(returnsSingle("null IS NULL", true))
    it("null IS NOT NULL")(returnsSingle("null IS NOT NULL", false))
    it("count(null)")(returnsSingle("count(null)", 0))
    it("collect(null)")(returnsSingle("collect(null)", List()))
  }
}
