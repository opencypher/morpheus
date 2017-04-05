package org.opencypher.spark.impl.ir

import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.{Neo4jAstTestSupport, StdTestSuite}

class GlobalsExtractorTest extends StdTestSuite with Neo4jAstTestSupport {

  test("extracts labels") {
    extracting("n:Foo") shouldRegisterLabel "Foo"
    extracting("true OR n:Foo AND r:Bar") shouldRegisterLabels ("Foo", "Bar")
    extracting("(:Foo)-->(:Bar)") shouldRegisterLabels ("Foo", "Bar")
  }

  test("extracts reltypes") {
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

  def extracting(expr: String): GlobalsMatcher = {
    val ast = parse(expr)
    GlobalsMatcher(GlobalsExtractor(ast))
  }

  case class GlobalsMatcher(registry: GlobalsRegistry) {
    def shouldRegisterLabel(name: String) = registry.label(name)
    def shouldRegisterLabels(names: String*) = names.foreach(registry.label)

    def shouldRegisterRelType(name: String) = registry.relType(name)
    def shouldRegisterRelTypes(names: String*) = names.foreach(registry.relType)

    def shouldRegisterPropertyKey(name: String) = registry.propertyKey(name)
    def shouldRegisterPropertyKeys(names: String*) = names.foreach(registry.propertyKey)

    def shouldRegisterConstant(name: String) = registry.constant(name)
    def shouldRegisterConstants(names: String*) = names.foreach(registry.constant)
  }

}
