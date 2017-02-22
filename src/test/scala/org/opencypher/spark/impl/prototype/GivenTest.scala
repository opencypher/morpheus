package org.opencypher.spark.impl.prototype

class GivenTest extends IrTestSupport {

  test("add connection") {
    Given.nothing
      .withConnection('r, DirectedRelationship('a, 'b)) should equal(
      Given(Map.empty, Map(toField('r) -> DirectedRelationship('a, 'b)))
    )
  }

  test("mark node as solved") {
    Given.nothing
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withEntity('r, AnyRelationship())
      .solvedNode('a) should equal(
        Given(Map(toField('b) -> AnyNode(), toField('r) -> AnyRelationship()), Map.empty)
    )
  }

  test("mark connection as solved") {
    Given.nothing
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withEntity('r, AnyRelationship())
      .withConnection('r, DirectedRelationship('a, 'b))
      .solvedConnection('r) should equal(
        Given(Map.empty, Map.empty)
    )
  }

}
