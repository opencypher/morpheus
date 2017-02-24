package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.ir.pattern.AllGiven

import scala.language.implicitConversions

class IrTestSuite extends StdTestSuite {

  implicit def toField(s: Symbol): Field = Field(s.name)
  implicit def toVar(s: Symbol): Var = Var(s.name)

  def irFor(root: Block[Expr]): CypherQuery[Expr] =
    irFor(BlockRef("root"), Map(BlockRef("root") -> root))

  def irFor(rootRef: BlockRef, blocks: Map[BlockRef, Block[Expr]]): CypherQuery[Expr] = {
    val root = blocks(rootRef)
    val result = ResultBlock[Expr](
      after = Set(rootRef),
      // TODO
      binds = OrderedFields[Expr](),
      where = AllGiven[Expr]()
    )
    val model = QueryModel(result, GlobalsRegistry.none, blocks)
    CypherQuery(QueryInfo("test"), model)
  }
}
