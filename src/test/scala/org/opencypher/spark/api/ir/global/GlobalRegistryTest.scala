package org.opencypher.spark.api.ir.global

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.impl.util.RefCollection

import scala.util.Try

class GlobalRegistryTest extends BaseTestSuite {

  val globals = GlobalsRegistry(
    TokenRegistry(
      labels = RefCollection(Vector(Label("Person"), Label("Employee"))),
      relTypes = RefCollection(Vector(RelType("KNOWS"))),
      propertyKeys = RefCollection(Vector(PropertyKey("prop")))
    ),
    ConstantRegistry(constants = RefCollection(Vector.empty))
  )

  import globals.tokens
  import globals.constants

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
      .empty
      .mapTokens(_.withLabel(Label("Person")))
      .mapTokens(_.withLabel(Label("Employee")))
      .mapTokens(_.withRelType(RelType("KNOWS")))
      .mapTokens(_.withPropertyKey(PropertyKey("prop"))) should equal(globals)
  }

  test("token definition is idempotent") {
    GlobalsRegistry
      .empty
      .mapTokens(_.withLabel(Label("Person")))
      .mapTokens(_.withLabel(Label("Person")))
      .mapTokens(_.withLabel(Label("Employee")))
      .mapTokens(_.withLabel(Label("Employee")))
      .mapTokens(_.withRelType(RelType("KNOWS")))
      .mapTokens(_.withRelType(RelType("KNOWS")))
      .mapTokens(_.withPropertyKey(PropertyKey("prop")))
      .mapTokens(_.withPropertyKey(PropertyKey("prop"))) should equal(globals)
  }
}
