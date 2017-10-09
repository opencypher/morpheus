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
package org.opencypher.caps.ir.impl.global

import org.opencypher.caps.ir.api.global._
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.Neo4jAstTestSupport

class ConstantsExtractorTest extends BaseTestSuite with Neo4jAstTestSupport {

  test("extracts constants") {
    extracting("$param") shouldRegisterConstant "param"
    extracting("$param OR n.prop + $c[$bar]") shouldRegisterConstants ("param", "c", "bar")
  }

  test("collect tokens") {
    val (given, _, _) = parseQuery("MATCH (a:Person)-[r:KNOWS]->(b:Duck) RETURN a.name, r.since, b.quack")
    val actual = ConstantsExtractor(given)
    val expected = ConstantRegistry.empty

    actual should equal(expected)
  }

  test("collect parameters") {
    val (given, _, _) = parseQuery("WITH $param AS p RETURN p, $another")
    val actual = ConstantsExtractor(given)
    val expected = ConstantRegistry.empty.withConstant(Constant("param")).withConstant(Constant("another"))

    actual should equal(expected)
  }

  private def extracting(expr: String): ConstantsMatcher = {
    val ast = parseExpr(expr)
    ConstantsMatcher(ConstantsExtractor(ast))
  }

  private case class ConstantsMatcher(registry: ConstantRegistry) {
    def shouldRegisterConstant(name: String) = registry.constantRefByName(name)
    def shouldRegisterConstants(names: String*) = names.foreach(registry.constantRefByName)
  }
}
