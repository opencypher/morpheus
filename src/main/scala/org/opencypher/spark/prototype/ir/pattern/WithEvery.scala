package org.opencypher.spark.prototype.ir.pattern

case object WithEvery {
  def of[T](elts: T*) = WithEvery(elts.toSet)
  def empty[T] = WithEvery[T](Set.empty)
}

final case class WithEvery[T](elts: Set[T])
