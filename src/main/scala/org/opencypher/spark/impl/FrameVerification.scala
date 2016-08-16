package org.opencypher.spark.impl

object FrameVerification {
  def unless[T <: FrameVerificationError](cond: => Boolean): Verifier[T] =
    new Verifier(cond)

  final class Verifier[T <: FrameVerificationError] private[impl] (cond: => Boolean) {
    def failWith(error: => T) = if (!cond) throw error
  }
}

abstract class FrameVerificationError(msg: String) extends AssertionError(msg) {
  self: Product with Serializable =>
}
