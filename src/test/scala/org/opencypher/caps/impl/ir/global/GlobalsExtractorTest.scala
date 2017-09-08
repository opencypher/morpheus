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
package org.opencypher.caps.impl.ir.global

import org.opencypher.caps.api.ir.global._
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.Neo4jAstTestSupport

class GlobalsExtractorTest extends BaseTestSuite with Neo4jAstTestSupport {

  test("extracts labels") {
    extracting("n:Foo") shouldRegisterLabel "Foo"
    extracting("$p OR n:Foo AND r:Bar") shouldRegisterLabels ("Foo", "Bar")
    extracting("(:Foo)-->(:Bar)") shouldRegisterLabels ("Foo", "Bar")
  }

  test("extracts rel types") {
    extracting("(:Foo)-[:TYPE]->()") shouldRegisterRelType "TYPE"
    extracting("(:Foo)-[r:TYPE]->()-->()<-[:SWEET]-()") shouldRegisterRelTypes ("TYPE", "SWEET")
  }

  test("extracts property keys") {
    extracting("n.prop") shouldRegisterPropertyKey "prop"
    extracting("n.prop AND r.foo = r.foo.bar") shouldRegisterPropertyKeys ("prop", "foo", "bar")
  }

  test("extracts constants") {
    extracting("$param") shouldRegisterConstant "param"
    extracting("$param OR n.prop + $c[$bar]") shouldRegisterConstants ("param", "c", "bar")
  }

  test("collect tokens") {
    val (given, _,_) = parseQuery("MATCH (a:Person)-[r:KNOWS]->(b:Duck) RETURN a.name, r.since, b.quack")
    val actual = GlobalsExtractor(given)
    val expected = GlobalsRegistry(
      TokenRegistry
      .empty
      .withLabel(Label("Duck"))
      .withLabel(Label("Person"))
      .withRelType(RelType("KNOWS"))
      .withPropertyKey(PropertyKey("name"))
      .withPropertyKey(PropertyKey("since"))
      .withPropertyKey(PropertyKey("quack"))
    )

    actual should equal(expected)
  }

  test("collect parameters") {
    val (given, _, _) = parseQuery("WITH $param AS p RETURN p, $another")
    val actual = GlobalsExtractor(given)
    val expected = GlobalsRegistry(
      TokenRegistry.empty,
      ConstantRegistry.empty.withConstant(Constant("param")).withConstant(Constant("another"))
    )

    actual should equal(expected)
  }

  private def extracting(expr: String): GlobalsMatcher = {
    val ast = parseExpr(expr)
    GlobalsMatcher(GlobalsExtractor(ast))
  }

  private case class GlobalsMatcher(registry: GlobalsRegistry) {
    def shouldRegisterLabel(name: String) = registry.tokens.labelRefByName(name)
    def shouldRegisterLabels(names: String*) = names.foreach(registry.tokens.labelRefByName)

    def shouldRegisterRelType(name: String) = registry.tokens.relTypeRefByName(name)
    def shouldRegisterRelTypes(names: String*) = names.foreach(registry.tokens.relTypeRefByName)

    def shouldRegisterPropertyKey(name: String) = registry.tokens.propertyKeyRefByName(name)
    def shouldRegisterPropertyKeys(names: String*) = names.foreach(registry.tokens.propertyKeyRefByName)

    def shouldRegisterConstant(name: String) = registry.constants.constantRefByName(name)
    def shouldRegisterConstants(names: String*) = names.foreach(registry.constants.constantRefByName)
  }
}
