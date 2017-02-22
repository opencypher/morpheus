package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir._

import scala.util.Try

class TokenRegistryTest extends StdTestSuite {

  val tokens = TokenRegistry(
    labels = Vector(LabelDef("Person"), LabelDef("Employee")),
    relTypes = Vector(RelTypeDef("KNOWS")),
    propertyKeys = Vector(PropertyKeyDef("prop"))
  )

  test("token lookup") {
    tokens.label(LabelRef(0)) should equal(LabelDef("Person"))
    tokens.label(LabelRef(1)) should equal(LabelDef("Employee"))
    Try(tokens.label(LabelRef(2))).toOption should equal(None)

    tokens.relType(RelTypeRef(0)) should equal(RelTypeDef("KNOWS"))
    Try(tokens.relType(RelTypeRef(1))).toOption should equal(None)

    tokens.propertyKey(PropertyKeyRef(0)) should equal(PropertyKeyDef("prop"))
    Try(tokens.propertyKey(PropertyKeyRef(1))).toOption should equal(None)
  }

  test("token definition") {
    TokenRegistry
      .none
      .withLabel(LabelDef("Person"))
      .withLabel(LabelDef("Employee"))
      .withRelType(RelTypeDef("KNOWS"))
      .withPropertyKey(PropertyKeyDef("prop")) should equal(tokens)
  }

  test("token definition is idempotent") {
    TokenRegistry
      .none
      .withLabel(LabelDef("Person"))
      .withLabel(LabelDef("Person"))
      .withLabel(LabelDef("Employee"))
      .withLabel(LabelDef("Employee"))
      .withRelType(RelTypeDef("KNOWS"))
      .withRelType(RelTypeDef("KNOWS"))
      .withPropertyKey(PropertyKeyDef("prop"))
      .withPropertyKey(PropertyKeyDef("prop")) should equal(tokens)
  }
}
