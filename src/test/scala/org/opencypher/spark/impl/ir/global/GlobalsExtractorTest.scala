package org.opencypher.spark.impl.ir.global

import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.{Neo4jAstTestSupport, TestSuiteImpl}

class GlobalsExtractorTest extends TestSuiteImpl with Neo4jAstTestSupport {

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
    val (given, _) = parseQuery("MATCH (a:Person)-[r:KNOWS]->(b:Duck) RETURN a.name, r.since, b.quack")
    val actual = GlobalsExtractor(given)
    val expected = GlobalsRegistry
      .empty
      .withLabel(Label("Duck"))
      .withLabel(Label("Person"))
      .withRelType(RelType("KNOWS"))
      .withPropertyKey(PropertyKey("name"))
      .withPropertyKey(PropertyKey("since"))
      .withPropertyKey(PropertyKey("quack"))

    actual should equal(expected)
  }

  test("collect parameters") {
    val (given, _) = parseQuery("WITH $param AS p RETURN p, $another")
    val actual = GlobalsExtractor(given)
    val expected = GlobalsRegistry.empty.withConstant(Constant("param")).withConstant(Constant("another"))

    actual should equal(expected)
  }

  private def extracting(expr: String): GlobalsMatcher = {
    val ast = parseExpr(expr)
    GlobalsMatcher(GlobalsExtractor(ast))
  }

  private case class GlobalsMatcher(registry: GlobalsRegistry) {
    def shouldRegisterLabel(name: String) = registry.labelRefByName(name)
    def shouldRegisterLabels(names: String*) = names.foreach(registry.labelRefByName)

    def shouldRegisterRelType(name: String) = registry.relTypeRefByName(name)
    def shouldRegisterRelTypes(names: String*) = names.foreach(registry.relTypeRefByName)

    def shouldRegisterPropertyKey(name: String) = registry.propertyKeyRefByName(name)
    def shouldRegisterPropertyKeys(names: String*) = names.foreach(registry.propertyKeyRefByName)

    def shouldRegisterConstant(name: String) = registry.constantRefByName(name)
    def shouldRegisterConstants(names: String*) = names.foreach(registry.constantRefByName)
  }
}
