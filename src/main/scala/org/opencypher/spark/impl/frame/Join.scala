package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl.{FrameVerificationError, StdCypherFrame, StdField, StdRuntimeContext}

object Join {

  import org.opencypher.spark.impl.FrameVerification._

  def apply(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
           (lhsKey: Symbol, rhsKey: Symbol): StdCypherFrame[Row] = {
    val lhsField = lhs.signature.field(lhsKey)
    val lhsType = lhsField.cypherType
    val rhsField = rhs.signature.field(rhsKey)
    val rhsType = rhsField.cypherType

    unless((lhsType meet rhsType).isInhabited.maybeTrue) failWith EmptyJoin(lhsType, rhsType)
    unless(lhs.signature.slot(lhsField).representation.isEmbedded) failWith NotEmbedded(lhsKey)
    unless(rhs.signature.slot(rhsField).representation.isEmbedded) failWith NotEmbedded(rhsKey)

    new Join(lhs, rhs)(lhsField, rhsField)
  }

  private final class Join(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
                          (lhsKey: StdField, rhsKey: StdField)
    extends StdCypherFrame[Row](lhs.signature ++ rhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val lhsSlot = signature.slot(lhsKey)
      val rhsSlot = signature.slot(rhsKey)

      val joinExpr = functions.expr(s"${lhsSlot.sym.name} = ${rhsSlot.sym.name}")

      lhsIn.join(rhsIn, joinExpr)
    }
  }

  protected[frame] final case class EmptyJoin(lhsType: CypherType, rhsType: CypherType)
    extends FrameVerificationError(s"Refusing to construct empty join between $lhsType and $rhsType join keys")

  protected[frame] final case class NotEmbedded(key: Symbol)
    extends FrameVerificationError(s"Cannot join on slot $key using non-embedded representation")
}
