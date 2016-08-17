package org.opencypher.spark.impl.util

import org.opencypher.spark.impl.error.{StdError, StdErrorInfo}

object Verification {

  val done = Some(())
  val fail = None

  final class Verifier[T <: Verification.Error, V] private[impl](value: => Option[V]) {
    def failWith(error: => T) = value.getOrElse(throw error)
  }

  abstract class Error(override val detail: String)(implicit private val info: StdErrorInfo)
    extends StdError(detail) {
    self: Product with Serializable =>
  }
}

trait Verification {

  import Verification._

  def ifNot[T <: Error](cond: => Boolean): Verifier[T, Unit] =
    new Verifier(if (cond) Verification.done else Verification.fail)

  def ifMissing[T <: Error, V](value: => Option[V]): Verifier[T, V] =
    new Verifier(value)
}






