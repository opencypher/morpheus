package org.opencypher.spark.impl

object FrameVerification extends FrameVerification

trait FrameVerification {
  def verify[T <: FrameVerificationError](cond: => Boolean) = {
    object verifier {
      def failWith(error: => T) = if (!cond) throw error
    }
    verifier
  }
}

abstract class FrameVerificationError(msg: String) extends AssertionError(msg) {
  self: Product with Serializable =>
}
