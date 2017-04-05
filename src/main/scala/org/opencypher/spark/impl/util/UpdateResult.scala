package org.opencypher.spark.impl.util

sealed trait UpdateResult[+T] {
  def it: T
}

// Non-Failed
sealed trait SuccessfulUpdateResult[+T] extends UpdateResult[T]

sealed trait FailedUpdateResult[+T] extends UpdateResult[T]

// Non-Removing
sealed trait AdditiveUpdateResult[+T] extends UpdateResult[T]

// Non-Additive
sealed trait RemovingUpdateResult[+T] extends UpdateResult[T]

// Neither Failed nor Found
sealed trait MutatingUpdateResult[+T] extends UpdateResult[T]

final case class Added[T](it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]

final case class Replaced[T](old: T, it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]

final case class Found[T](it: T)
  extends SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]

final case class FailedToAdd[T](
  conflict: T,
  update: SuccessfulUpdateResult[T] with AdditiveUpdateResult[T] with MutatingUpdateResult[T]
) extends FailedUpdateResult[T] with AdditiveUpdateResult[T] {

  def it = update.it
}

final case class Removed[T](it: T, dependents: Set[T] = Set.empty)
  extends SuccessfulUpdateResult[T] with MutatingUpdateResult[T] with RemovingUpdateResult[T]

final case class NotFound[T](it: T)
  extends SuccessfulUpdateResult[T] with RemovingUpdateResult[T]

final case class FailedToRemove[T](
 conflict: T,
 update: SuccessfulUpdateResult[T] with RemovingUpdateResult[T] with MutatingUpdateResult[T]
) extends FailedUpdateResult[T] with RemovingUpdateResult[T] {

  def it = update.it
}
