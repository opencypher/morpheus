package org.opencypher.spark.impl.verify

object Verifier {
  val pass = Some(())
  val fail = None
}

final class Verifier[T <: Verification.Error, V] private[impl](value: => Option[V]) {
  def failWith(error: => T) = value.getOrElse(throw error)
}
