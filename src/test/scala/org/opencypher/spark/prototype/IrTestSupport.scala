package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir.block.{Block, BlockRef}
import org.opencypher.spark.prototype.ir.token.TokenRegistry
import org.opencypher.spark.prototype.ir.{Field, QueryDescriptor, QueryInfo, QueryModel}

import scala.collection.immutable.SortedMap

class IrTestSupport extends StdTestSuite {

  implicit def toField(s: Symbol): Field = Field(s.name)
  implicit def toVar(s: Symbol): Var = Var(s.name)

  def irFor(root: Block[Expr]): QueryDescriptor[Expr] = {
    val model = QueryModel(BlockRef("root"), TokenRegistry.none, Map(BlockRef("root") -> root))
    QueryDescriptor(QueryInfo("test"), model, SortedMap.empty)
  }
}
