package org.opencypher.spark.prototype.ir.pattern

case object WithAny {
  def of[T](elts: T*) = WithAny(elts.toSet)
  def empty[T] = WithAny[T](Set.empty)
}

final case class WithAny[T](elts: Set[T])
