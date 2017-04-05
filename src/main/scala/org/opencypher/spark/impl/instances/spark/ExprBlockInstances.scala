package org.opencypher.spark.impl.instances.spark

import org.opencypher.spark.api.expr.Expr
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.block.MatchBlock
import org.opencypher.spark.impl.classes.TypedBlock

trait ExprBlockInstances {

  implicit val typedMatchBlock = new TypedBlock[MatchBlock[Expr]] {

    override type BlockExpr = Expr

    override def outputs(block: MatchBlock[Expr]): Set[Field] = ???
  }
}
