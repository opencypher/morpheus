package org.opencypher.spark.api.ir.global

import org.opencypher.spark.StdTestSuite

import scala.util.Try

class GlobalRegistryTest extends StdTestSuite {

  val tokens = GlobalsRegistry(
    labels = Vector(Label("Person"), Label("Employee")),
    relTypes = Vector(RelType("KNOWS")),
    propertyKeys = Vector(PropertyKey("prop")),
    constants = Vector.empty
  )

  test("token lookup") {
    tokens.label(LabelRef(0)) should equal(Label("Person"))
    tokens.label(LabelRef(1)) should equal(Label("Employee"))
    Try(tokens.label(LabelRef(2))).toOption should equal(None)

    tokens.relType(RelTypeRef(0)) should equal(RelType("KNOWS"))
    Try(tokens.relType(RelTypeRef(1))).toOption should equal(None)

    tokens.propertyKey(PropertyKeyRef(0)) should equal(PropertyKey("prop"))
    Try(tokens.propertyKey(PropertyKeyRef(1))).toOption should equal(None)
  }

  test("token definition") {
    GlobalsRegistry
      .none
      .withLabel(Label("Person"))
      .withLabel(Label("Employee"))
      .withRelType(RelType("KNOWS"))
      .withPropertyKey(PropertyKey("prop")) should equal(tokens)
  }

  test("token definition is idempotent") {
    GlobalsRegistry
      .none
      .withLabel(Label("Person"))
      .withLabel(Label("Person"))
      .withLabel(Label("Employee"))
      .withLabel(Label("Employee"))
      .withRelType(RelType("KNOWS"))
      .withRelType(RelType("KNOWS"))
      .withPropertyKey(PropertyKey("prop"))
      .withPropertyKey(PropertyKey("prop")) should equal(tokens)
  }
}
