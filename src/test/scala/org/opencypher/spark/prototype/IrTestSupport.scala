package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.token.TokenRegistry

import scala.language.implicitConversions

class IrTestSupport extends StdTestSuite {

  implicit def toField(s: Symbol): Field = Field(s.name)
  implicit def toVar(s: Symbol): Var = Var(s.name)

  def irFor(root: Block[Expr]): QueryDescriptor[Expr] = {
    val result = ResultBlock[Expr](
      after = Set(BlockRef("root")),
      over = root.over,
      binds = ResultFields(root.over.outputs.toSeq),
      where = Where.everything
    )
    val model = QueryModel(result, TokenRegistry.none, Map(BlockRef("root") -> root))
    QueryDescriptor(QueryInfo("test"), model)
  }
}
