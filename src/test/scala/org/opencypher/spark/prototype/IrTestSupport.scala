package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block.{Block, BlockRef}
import org.opencypher.spark.prototype.ir.token.TokenRegistry

class IrTestSupport extends StdTestSuite {

  implicit def toField(s: Symbol): Field = Field(s.name)
  implicit def toVar(s: Symbol): Var = Var(s.name)

  def irFor(root: Block[Expr]): QueryDescriptor[Expr] = {
    val model = QueryModel(BlockRef("root"), TokenRegistry.none, Map(BlockRef("root") -> root))
    QueryDescriptor(QueryInfo("test"), model, QueryReturns.nothing)
  }
}
