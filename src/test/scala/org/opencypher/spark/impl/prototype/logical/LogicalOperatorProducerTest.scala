package org.opencypher.spark.impl.prototype.logical

import org.opencypher.spark.impl.prototype._

class LogicalOperatorProducerTest extends IrTestSupport {

  test("convert match block") {
    val given = Given.nothing
      .withEntity('a, AnyNode())
      .withEntity('b, AnyNode())
      .withConnection('r, DirectedRelationship('a, 'b))

    val block = matchBlock(given)

    plan(block) should equal(
      ExpandSource('a, 'r, 'b, NodeScan('a, AnyNode()))
    )
  }

  private def matchBlock(given: Given): BlockDef[Expr] =
    MatchBlock[Expr](Set.empty, BlockSignature.empty, given, Where.everything[Expr])

  private val producer = new LogicalOperatorProducer

  private def plan(block: BlockDef[Expr]): LogicalOperator = {
    val structure = BlockStructure(Map(BlockRef("block") -> block), BlockRef("block"))
    val root = RootBlockImpl(Set.empty, Set.empty, Set.empty, TokenDefs.none, structure)
    plan(new TestIR(root))
  }

  private def plan(ir: QueryRepresentation[Expr]): LogicalOperator =
    producer.plan(ir)
}
