package org.opencypher.spark.impl.prototype

import org.opencypher.spark.StdTestSuite

class TokenDefsTest extends StdTestSuite {

  val tokens = TokenDefs(
    labels = Vector(LabelDef("Person"), LabelDef("Employee")),
    relTypes = Vector(RelTypeDef("KNOWS")),
    propertyKeys = Vector(PropertyKeyDef("prop"))
  )

  test("token lookup") {
    tokens.label(LabelRef(0)).get should equal(LabelDef("Person"))
    tokens.label(LabelRef(1)).get should equal(LabelDef("Employee"))
    tokens.label(LabelRef(2)) should equal(None)

    tokens.relType(RelTypeRef(0)).get should equal(RelTypeDef("KNOWS"))
    tokens.relType(RelTypeRef(1)) should equal(None)

    tokens.propertyKey(PropertyKeyRef(0)).get should equal(PropertyKeyDef("prop"))
    tokens.propertyKey(PropertyKeyRef(1)) should equal(None)
  }

  test("token definition") {
    TokenDefs
      .empty
      .withLabel(LabelDef("Person"))._2
      .withLabel(LabelDef("Employee"))._2
      .withRelType(RelTypeDef("KNOWS"))._2
      .withPropertyKey(PropertyKeyDef("prop"))._2 should equal(tokens)
  }

  test("token definition is idempotent") {
    TokenDefs
      .empty
      .withLabel(LabelDef("Person"))._2
      .withLabel(LabelDef("Person"))._2
      .withLabel(LabelDef("Employee"))._2
      .withLabel(LabelDef("Employee"))._2
      .withRelType(RelTypeDef("KNOWS"))._2
      .withRelType(RelTypeDef("KNOWS"))._2
      .withPropertyKey(PropertyKeyDef("prop"))._2
      .withPropertyKey(PropertyKeyDef("prop"))._2 should equal(tokens)
  }
}
