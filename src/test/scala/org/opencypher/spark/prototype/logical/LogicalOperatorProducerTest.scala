package org.opencypher.spark.prototype.logical

import org.opencypher.spark.prototype._
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.pattern.{AnyNode, DirectedRelationship, Pattern}

class LogicalOperatorProducerTest extends IrTestSupport {

  test("convert match block") {
    val pattern = Pattern.empty[Expr]
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withConnection('r, DirectedRelationship('a, 'b))

    val block = matchBlock(pattern)

    plan(block) should equal(
      ExpandSource('a, 'r, 'b, NodeScan('a, AnyNode()))
    )
  }

  private def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](Set.empty, BlockSig.empty, pattern, Where.everything[Expr])

  private val producer = new LogicalOperatorProducer

  private def plan(block: Block[Expr]): LogicalOperator = {
    plan(irFor(block))
  }

  private def plan(ir: QueryDescriptor[Expr]): LogicalOperator =
    producer.plan(ir)
}
