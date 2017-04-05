package org.opencypher.spark.legacy.impl.verify

import org.opencypher.spark.legacy.impl.error.{StdError, StdErrorInfo}
import org.opencypher.spark.legacy.impl.verify.Verification.SupposedlyImpossible

object Verification extends Verification {
  abstract class Error(override val detail: String)(implicit private val info: StdErrorInfo)
    extends StdError(detail) {
    self: Product with Serializable =>
  }

  final case class UnObtainable[A](arg: A)(implicit info: StdErrorInfo)
    extends Error(s"Cannot obtain ${info.enclosing} '$arg'")

  final case class SupposedlyImpossible(msg: String)(implicit info: StdErrorInfo)
    extends Error(msg)
}

trait Verification {

  protected def ifNot[T <: Verification.Error](cond: => Boolean): Verifier[T, Unit] =
    new Verifier(if (cond) Verifier.pass else Verifier.fail)

  protected def ifMissing[T <: Verification.Error, V](value: => Option[V]): Verifier[T, V] =
    new Verifier(value)

  protected def ifExists[T <: Verification.Error, V](value: => Option[V]): Verifier[T, Unit] =
    ifNot(value.isEmpty)

  protected def obtain[A, T](value: A => Option[T])(arg: A)(implicit info: StdErrorInfo): T =
    ifMissing(value(arg)) failWith Verification.UnObtainable(arg)(info)

  protected def supposedlyImpossible(msg: String)(implicit info: StdErrorInfo) =
    throw SupposedlyImpossible(msg)
}



