package org.opencypher.spark.api.ir.pattern

import org.opencypher.spark.api.expr.Expr
import org.opencypher.spark.impl.ir.IrTestSuite
import org.opencypher.spark.toField

class PatternTest extends IrTestSuite {

  test("add connection") {
    Pattern.empty[Expr]
      .withConnection('r, DirectedRelationship('a, 'b)) should equal(
      Pattern(Map.empty, Map(toField('r) -> DirectedRelationship('a, 'b)))
    )
  }

  test("mark node as solved") {
    Pattern.empty[Expr]
      .withEntity('a, EveryNode)
      .withEntity('b, EveryNode)
      .withEntity('r, EveryRelationship)
      .solvedNode('a) should equal(
        Pattern(Map(toField('b) -> EveryNode, toField('r) -> EveryRelationship), Map.empty)
    )
  }

  test("mark connection as solved") {
    Pattern.empty[Expr]
      .withEntity('a, EveryNode)
      .withEntity('b, EveryNode)
      .withEntity('r, EveryRelationship)
      .withConnection('r, DirectedRelationship('a, 'b))
      .withoutConnection('r) should equal(
        Pattern(Map.empty, Map.empty)
    )
  }
}
