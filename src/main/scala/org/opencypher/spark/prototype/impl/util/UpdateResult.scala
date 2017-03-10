package org.opencypher.spark.prototype.impl.util

sealed trait UpdateResult[+T] {
  def it: T
}

// Non-Failed
sealed trait SuccessfulUpdateResult[+T] extends UpdateResult[T]

// Non-Deleting
sealed trait AdditiveUpdateResult[+T] extends UpdateResult[T]

// Neither Failed nor Found
sealed trait MutatingUpdateResult[+T] extends UpdateResult[T]

final case class Added[T](it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]

final case class Replaced[T](old: T, it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]

final case class Found[T](it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]

final case class Deleted[T](it: T)
  extends SuccessfulUpdateResult[T] with MutatingUpdateResult[T]

final case class Failed[T](
  conflict: T,
  update: SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]
) extends AdditiveUpdateResult[T] {

  def it = update.it
}
