package org.opencypher.spark.impl.ir.block

import org.opencypher.spark.api.expr.Expr
import org.opencypher.spark.api.ir.block.MatchBlock
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.impl.instances.ir._
import org.opencypher.spark.impl.ir.IrTestSuite
import org.opencypher.spark.impl.syntax.block._


class TypedMatchBlockTest extends IrTestSuite {

  test("computes typed output fields") {
    val block = matchBlock("MATCH (n:Person) RETURN n")

    block.outputs.map(_.toTypedTuple) should equal(Set("n" -> CTNode("Person")))
  }

  private def matchBlock(singleMatchQuery: String): MatchBlock[Expr] = {
    val model = singleMatchQuery.ir.model
    val projectBlockRef = model.result.after.head
    val matchBlockRef = model.blocks(projectBlockRef).after.head

    model.blocks(matchBlockRef) match {
      case block: MatchBlock[Expr] =>
        block

      case x => throw new MatchError(s"Supposed to be a match block, was: $x")
    }
  }
}
