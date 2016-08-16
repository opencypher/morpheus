package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.{FrameVerificationError, StdCypherFrame, StdRuntimeContext}

object UnionAll {

  import org.opencypher.spark.impl.FrameVerification._

  def apply(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product]): StdCypherFrame[Product] = {
    unless(lhs.signature.fields.equals(rhs.signature.fields)) failWith SignatureMismatch {
      s"""Fields must be equal in UNION
          |${lhs.signature.fields}
          |${rhs.signature.fields}
      """.stripMargin
    }

    new UnionAll(lhs, rhs)
  }

  private final class UnionAll(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product])
    extends StdCypherFrame[Product](lhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val union = lhsIn.union(rhsIn)

      union
    }
  }

  protected[frame] final case class SignatureMismatch(msg: String) extends FrameVerificationError(msg)
}
