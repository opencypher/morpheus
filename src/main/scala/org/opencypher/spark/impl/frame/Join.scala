package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdRuntimeContext}

object Join {

  def apply(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row], lhsKey: StdField, rhsKey: StdField): StdCypherFrame[Row] = new Join(lhs, rhs, lhsKey, rhsKey)

  private final class Join(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row], lhsKey: StdField, rhsKey: StdField) extends StdCypherFrame[Row](lhs.signature ++ rhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val lhsSlot = signature.slot(lhsKey)
      val rhsSlot = signature.slot(rhsKey)

      val joinExpr = functions.expr(s"${lhsSlot.sym.name} = ${rhsSlot.sym.name}")

      lhsIn.join(rhsIn, joinExpr)
    }
  }
}
