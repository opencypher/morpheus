package org.opencypher.spark.prototype.logical

import org.opencypher.spark.prototype._
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.pattern.{AllGiven, EveryNode, DirectedRelationship, Pattern}

class LogicalOperatorProducerTest extends IrTestSuite {

  test("convert match block") {
    val pattern = Pattern.empty[Expr]
      .withEntity('a, EveryNode())
      .withEntity('b, EveryNode())
      .withConnection('r, DirectedRelationship('a, 'b))

    val block = matchBlock(pattern)

    plan(block) should equal(
      ExpandSource('a, 'r, 'b, NodeScan('a, EveryNode()))
    )
  }

  private def matchBlock(pattern: Pattern[Expr]): Block[Expr] =
    MatchBlock[Expr](Set.empty, BlockSig.empty, pattern, AllGiven[Expr]())

  private val producer = new LogicalOperatorProducer

  private def plan(block: Block[Expr]): LogicalOperator = {
    plan(irFor(block))
  }

  private def plan(ir: CypherQuery[Expr]): LogicalOperator =
    producer.plan(ir)
}
