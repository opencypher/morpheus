package org.opencypher.spark.prototype

import org.opencypher.spark.prototype.ir.pattern.{AnyNode, AnyRelationship, DirectedRelationship, Pattern}

class PatternTest extends IrTestSupport {

  test("add connection") {
    Pattern.empty[Expr]
      .withConnection('r, DirectedRelationship('a, 'b)) should equal(
      Pattern(Map.empty, Map(toField('r) -> DirectedRelationship('a, 'b)))
    )
  }

  test("mark node as solved") {
    Pattern.empty[Expr]
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withEntity('r, AnyRelationship())
      .solvedNode('a) should equal(
        Pattern(Map(toField('b) -> AnyNode(), toField('r) -> AnyRelationship()), Map.empty)
    )
  }

  test("mark connection as solved") {
    Pattern.empty[Expr]
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withEntity('r, AnyRelationship())
      .withConnection('r, DirectedRelationship('a, 'b))
      .solvedConnection('r) should equal(
        Pattern(Map.empty, Map.empty)
    )
  }

}
